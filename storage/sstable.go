package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

type (
	SSTable struct {
		db           *DB
		level        int
		fileName     string
		keyIndex     *KeyIndex
		keyIndexFile *os.File
		dataFile     *os.File
		minKey       string
		maxKey       string
		size         int64

		prev *SSTable
		next *SSTable
	}

	// TODO: use mmap to minimize user mode memory
	KeyIndex struct {
		sparseIndex []KeyIndexElement
	}

	KeyIndexElement struct {
		key        string
		dataOffset int64 // offset in dataFile
	}
)

const (
	KeyIndexElementMetaSize = 10 // key offset 8 byte, key length 2 byte

	SparseIndexInterval = 10
)

// level 0 SSTable
func newSSTableFromMemTable(memTable *MemTable) (*SSTable, error) {

	dataElemChan := make(chan *DataElement)
	go func() {
		memTable.dataIterate(func(dataElem *DataElement) error {
			dataElemChan <- dataElem
			return nil
		})
		close(dataElemChan)
	}()

	return newSSTableFromChannel(memTable.db, LevelZero, memTable.fileName, dataElemChan)
}

func compactSSTables(ssTable *SSTable, nextLevelSSTables []*SSTable, fileName string) (*SSTable, error) {

	if len(nextLevelSSTables) == 0 {
		return ssTable.upgrade(ssTable.level + 1)
	}

	lowLevelDataElemChan := make(chan *DataElement)
	highLevelDataElemChan := make(chan *DataElement)
	mergedDataElemChan := make(chan *DataElement)
	go func() {
		ssTable.dataIterate(func(dataElem *DataElement) error {
			lowLevelDataElemChan <- dataElem
			return nil
		})
		close(lowLevelDataElemChan)
	}()
	go func() {
		for _, nextLevelSSTable := range nextLevelSSTables {
			nextLevelSSTable.dataIterate(func(dataElem *DataElement) error {
				highLevelDataElemChan <- dataElem
				return nil
			})
		}
		close(highLevelDataElemChan)
	}()
	go func() {

		lowLevelDataElemEjected, highLevelDataElemEjected := true, true
		lowLevelDataValid, highLevelDataValid := true, true
		var (
			lowLevelDataElem  *DataElement
			highLevelDataElem *DataElement
		)

		for lowLevelDataValid && highLevelDataValid {
			if lowLevelDataElemEjected {
				lowLevelDataElem, lowLevelDataValid = <-lowLevelDataElemChan
				lowLevelDataElemEjected = !lowLevelDataValid
			}
			if highLevelDataElemEjected {
				highLevelDataElem, highLevelDataValid = <-highLevelDataElemChan
				highLevelDataElemEjected = !highLevelDataValid
			}

			if !lowLevelDataValid || !highLevelDataValid {
				if !lowLevelDataElemEjected {
					mergedDataElemChan <- lowLevelDataElem
					lowLevelDataElemEjected = true
				} else if !highLevelDataElemEjected {
					mergedDataElemChan <- highLevelDataElem
					highLevelDataElemEjected = true
				}
				break
			}

			if lowLevelDataElem.key < highLevelDataElem.key {
				mergedDataElemChan <- lowLevelDataElem
				lowLevelDataElemEjected = true
			} else if lowLevelDataElem.key > highLevelDataElem.key {
				mergedDataElemChan <- highLevelDataElem
				highLevelDataElemEjected = true
			} else {
				// lower level has newer k/v
				mergedDataElemChan <- lowLevelDataElem
				lowLevelDataElemEjected = true
				highLevelDataElemEjected = true
			}
		}

		// one of these two channels has closed
		// only one of these two loops will be executed
		for highLevelDataElem := range highLevelDataElemChan {
			mergedDataElemChan <- highLevelDataElem
		}
		for lowLevelDataElem := range lowLevelDataElemChan {
			mergedDataElemChan <- lowLevelDataElem
		}

		close(mergedDataElemChan)
	}()
	return newSSTableFromChannel(ssTable.db, ssTable.level+1, fileName, mergedDataElemChan)
}

func newSSTableFromChannel(db *DB, level int, fileName string, dataElemChan chan *DataElement) (*SSTable, error) {

	keyIndexPath := filepath.Join(getLevelSSTableKeyIndexDir(db.dbDir, level), fileName)
	dataPath := filepath.Join(getLevelSSTableDataDir(db.dbDir, level), fileName)
	tmpKeyIndexPath := filepath.Join(getSSTableTmpKeyIndexDir(db.dbDir), fileName)
	tmpDataPath := filepath.Join(getSSTableTmpDataDir(db.dbDir), fileName)

	tmpKeyIndexFile, err := os.OpenFile(tmpKeyIndexPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		return nil, err
	}

	tmpDataFile, err := os.OpenFile(tmpDataPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		return nil, err
	}

	keyIndex := KeyIndex{}
	size := int64(0)

	idx := 0
	dataOffset := int64(0)
	var dataElem *DataElement
	for dataElem = range dataElemChan {

		size += int64(dataElem.Size())

		_, err := tmpDataFile.Write(dataElem.Serialization())
		if err != nil {
			return nil, err
		}

		if idx%SparseIndexInterval == 0 {

			elem := KeyIndexElement{
				key:        dataElem.key,
				dataOffset: dataOffset,
			}
			keyIndex.sparseIndex = append(keyIndex.sparseIndex, elem)
			_, err := tmpKeyIndexFile.Write(elem.Serialization())
			if err != nil {
				return nil, err
			}
		}

		dataOffset += int64(dataElem.Size())
		idx++
	}

	// make sure sparse index end with last element
	lastDataElem := dataElem
	if keyIndex.sparseIndex[len(keyIndex.sparseIndex)-1].key != lastDataElem.key {
		elem := KeyIndexElement{
			key:        dataElem.key,
			dataOffset: dataOffset,
		}
		keyIndex.sparseIndex = append(keyIndex.sparseIndex, elem)
		_, err := tmpKeyIndexFile.Write(elem.Serialization())
		if err != nil {
			return nil, err
		}
	}

	var (
		keyIndexFile *os.File
		dataFile     *os.File
	)

	// mv from tmp to formal
	for _, f := range []func() error{
		func() error { return tmpKeyIndexFile.Close() },
		func() error { return tmpDataFile.Close() },
		func() error { return os.Rename(tmpKeyIndexPath, keyIndexPath) },
		func() error { return os.Rename(tmpDataPath, dataPath) },
		func() (err error) { keyIndexFile, err = os.Open(keyIndexPath); return },
		func() (err error) { dataFile, err = os.Open(dataPath); return },
	} {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &SSTable{
		db:           db,
		level:        level,
		fileName:     fileName,
		keyIndex:     &keyIndex,
		keyIndexFile: keyIndexFile,
		dataFile:     dataFile,
		minKey:       keyIndex.sparseIndex[0].key,
		maxKey:       keyIndex.sparseIndex[len(keyIndex.sparseIndex)-1].key,
		size:         size,
	}, nil
}

func restoreSSTable(db *DB, level int, fileName string) (*SSTable, error) {

	keyIndexPath := filepath.Join(getLevelSSTableKeyIndexDir(db.dbDir, level), fileName)
	dataPath := filepath.Join(getLevelSSTableDataDir(db.dbDir, level), fileName)

	keyIndexFile, err := os.Open(keyIndexPath)
	if err != nil {
		return nil, err
	}

	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}

	dataFileInfo, err := os.Stat(dataPath)
	if err != nil {
		return nil, err
	}

	keyIndexScanner := bufio.NewScanner(keyIndexFile)
	keyIndexScanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {

		elem := KeyIndexElement{}
		if err := elem.Deserialization(data); err != nil {
			return 0, nil, nil
		}

		advance = elem.Size()
		token = data[:advance]
		return
	})

	keyIndex := KeyIndex{}

	for keyIndexScanner.Scan() {

		elem := KeyIndexElement{}
		if err := elem.Deserialization(keyIndexScanner.Bytes()); err != nil {
			return nil, err
		}

		keyIndex.sparseIndex = append(keyIndex.sparseIndex, elem)
	}

	if err := keyIndexScanner.Err(); err != nil {
		return nil, err
	}

	return &SSTable{
		db:           db,
		level:        level,
		fileName:     fileName,
		keyIndex:     &keyIndex,
		keyIndexFile: keyIndexFile,
		dataFile:     dataFile,
		minKey:       keyIndex.sparseIndex[0].key,
		maxKey:       keyIndex.sparseIndex[len(keyIndex.sparseIndex)-1].key,
		size:         dataFileInfo.Size(),
	}, nil
}

func (ssTable *SSTable) finalize() {
	ssTable.keyIndexFile.Close()
	ssTable.dataFile.Close()
	os.Remove(ssTable.keyIndexPath())
	os.Remove(ssTable.dataPath())
}

func (ssTable *SSTable) keyIndexPath() string {
	return filepath.Join(getLevelSSTableKeyIndexDir(ssTable.db.dbDir, ssTable.level), ssTable.fileName)
}

func (ssTable *SSTable) dataPath() string {
	return filepath.Join(getLevelSSTableDataDir(ssTable.db.dbDir, ssTable.level), ssTable.fileName)
}

func (ssTable *SSTable) upgrade(level int) (*SSTable, error) {

	newKeyIndexPath := filepath.Join(getLevelSSTableKeyIndexDir(ssTable.db.dbDir, level), ssTable.fileName)
	newDataPath := filepath.Join(getLevelSSTableDataDir(ssTable.db.dbDir, level), ssTable.fileName)
	var (
		keyIndexFile *os.File
		dataFile     *os.File
	)
	for _, f := range []func() error{
		func() error { return os.Link(ssTable.keyIndexPath(), newKeyIndexPath) },
		func() error { return os.Link(ssTable.dataPath(), newDataPath) },
		func() (err error) { keyIndexFile, err = os.Open(newKeyIndexPath); return },
		func() (err error) { dataFile, err = os.Open(newDataPath); return },
	} {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &SSTable{
		db:           ssTable.db,
		level:        level,
		fileName:     ssTable.fileName,
		keyIndex:     ssTable.keyIndex,
		keyIndexFile: keyIndexFile,
		dataFile:     dataFile,
		minKey:       ssTable.minKey,
		maxKey:       ssTable.maxKey,
		size:         ssTable.size,
	}, nil
}

func (ssTable *SSTable) dataIterate(f func(dataElem *DataElement) error) error {
	dataPath := ssTable.dataPath()
	// use a new fd to avoid read concurrently
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return err
	}

	dataScanner := bufio.NewScanner(dataFile)
	dataScanner.Split(dataElementScannerSplit)

	for dataScanner.Scan() {
		dataElem := DataElement{}
		if err := dataElem.Deserialization(dataScanner.Bytes()); err != nil {
			return err
		}
		if err := f(&dataElem); err != nil {
			return err
		}
	}

	return dataScanner.Err()
}

func (ssTable *SSTable) search(key string) ([]byte, bool, error) {

	leftElem, rightElem := ssTable.keyIndex.searchInterval(key)
	if leftElem == nil || rightElem == nil {
		return nil, false, nil
	}

	ssTable.dataFile.Seek(leftElem.dataOffset, io.SeekStart)
	dataScanner := bufio.NewScanner(ssTable.dataFile)
	dataScanner.Split(dataElementScannerSplit)

	offset := leftElem.dataOffset
	for dataScanner.Scan() && offset <= rightElem.dataOffset {
		dataElem := DataElement{}
		if err := dataElem.Deserialization(dataScanner.Bytes()); err != nil {
			return nil, false, err
		}
		if dataElem.key == key {
			if dataElem.isTombstone {
				return nil, true, nil
			}
			return dataElem.value, true, nil
		}
		offset += int64(dataElem.Size())
	}

	if err := dataScanner.Err(); err != nil {
		return nil, false, err
	}

	return nil, false, nil
}

func (ki *KeyIndex) searchInterval(key string) (leftElem *KeyIndexElement, rightElem *KeyIndexElement) {

	leftIdx, rightIdx := 0, len(ki.sparseIndex)-1
	if ki.sparseIndex[leftIdx].key > key || ki.sparseIndex[rightIdx].key < key {
		return nil, nil
	}

	// binary search
	for leftIdx+1 < rightIdx {

		midIdx := leftIdx + (rightIdx-leftIdx)/2
		if ki.sparseIndex[midIdx].key < key {
			leftIdx = midIdx
		} else if ki.sparseIndex[midIdx].key > key {
			rightIdx = midIdx
		} else {
			return &ki.sparseIndex[midIdx], &ki.sparseIndex[midIdx]
		}
	}

	return &ki.sparseIndex[leftIdx], &ki.sparseIndex[rightIdx]
}

func (kie *KeyIndexElement) Serialization() []byte {

	keyLen := len(kie.key)
	buf := make([]byte, KeyIndexElementMetaSize, KeyIndexElementMetaSize+keyLen)

	buf[0] = byte(kie.dataOffset >> 56)
	buf[1] = byte(kie.dataOffset >> 48)
	buf[2] = byte(kie.dataOffset >> 40)
	buf[3] = byte(kie.dataOffset >> 32)
	buf[4] = byte(kie.dataOffset >> 24)
	buf[5] = byte(kie.dataOffset >> 16)
	buf[6] = byte(kie.dataOffset >> 8)
	buf[7] = byte(kie.dataOffset)

	buf[8] = byte(keyLen >> 8)
	buf[9] = byte(keyLen)

	buf = append(buf, []byte(kie.key)...)
	return buf
}

func (kie *KeyIndexElement) Deserialization(buf []byte) error {

	if len(buf) < KeyIndexElementMetaSize {
		return ErrIncompleteElement
	}

	dataOffset := int64(binary.BigEndian.Uint64(buf[:8]))
	keyLen := int(binary.BigEndian.Uint16(buf[8:10]))
	if len(buf) < KeyIndexElementMetaSize+keyLen {
		return ErrIncompleteElement
	}

	kie.key = string(buf[KeyIndexElementMetaSize : KeyIndexElementMetaSize+keyLen])
	kie.dataOffset = dataOffset

	return nil
}

func (kie *KeyIndexElement) Size() int {
	return len(kie.key) + KeyIndexElementMetaSize
}
