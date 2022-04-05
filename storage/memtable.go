package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type (
	MemTable struct {
		db       *DB
		fileName string
		maxSize  int

		currCount int
		currSize  int
		dataStore *DataStore
		walFile   *os.File

		prev *MemTable
		next *MemTable
	}

	// skip list
	DataStore struct {
		root      *DataNode
		r         *rand.Rand
		currLayer int
		maxLayer  int
	}

	DataNode struct {
		key     string
		element *DataElement
		next    []*DataNode
	}

	DataElement struct {
		key         string
		value       []byte
		isTombstone bool
	}
)

const (
	MemTableMaxSize  = 1024 * 1024 // 1 MB
	MemTableMaxLayer = 16          // skip list layer
)

const (
	DataElementMetaSize = 7 // type 1 byte, key length 2 byte, value length 4 byte
	MaxKeyLength        = 1<<16 - 1
	MaxValueLength      = 1<<32 - 1
)

var (
	ErrIncompleteElement = errors.New("incomplete element")
)

func newMemTable(db *DB, fileName string) (*MemTable, error) {

	walFile, err := os.OpenFile(filepath.Join(getMemTableWALDir(db.dbDir), fileName),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		return nil, err
	}

	return &MemTable{
		db:        db,
		fileName:  fileName,
		maxSize:   MemTableMaxSize,
		dataStore: newDataStore(MemTableMaxLayer),
		walFile:   walFile,
	}, nil
}

func (mt *MemTable) isFull() bool {
	return mt.currSize >= mt.maxSize
}

func (mt *MemTable) finalize() {
	mt.walFile.Close()
	os.Remove(mt.walPath())
}

func (mt *MemTable) walPath() string {
	return filepath.Join(getMemTableWALDir(mt.db.dbDir), mt.fileName)
}

func (mt *MemTable) search(key string) ([]byte, bool) {
	dataElem := mt.dataStore.search(key)
	if dataElem == nil {
		return nil, false
	}
	if dataElem.isTombstone {
		return nil, true
	}
	return dataElem.value, true
}

func (mt *MemTable) insertOrUpdate(key string, value []byte) error {
	return mt.insertOrUpdateDataElement(&DataElement{
		key:   key,
		value: value,
	})
}

func (mt *MemTable) del(key string) error {
	return mt.insertOrUpdateDataElement(&DataElement{
		key:         key,
		isTombstone: true,
	})
}

func (mt *MemTable) restoreFromWAL() error {

	walFile, err := os.Open(mt.walPath())
	if err != nil {
		return err
	}

	logScanner := bufio.NewScanner(walFile)
	logScanner.Split(dataElementScannerSplit)

	for logScanner.Scan() {

		dataElem := DataElement{}
		err := dataElem.Deserialization(logScanner.Bytes())
		if err != nil {
			return err
		}

		mt.insertOrUpdateMemory(&dataElem)
	}

	return logScanner.Err()
}

func (mt *MemTable) insertOrUpdateDataElement(dataElem *DataElement) error {
	mt.insertOrUpdateMemory(dataElem)
	return mt.writeAheadLog(dataElem)
}

func (mt *MemTable) insertOrUpdateMemory(dataElem *DataElement) {
	oldDataElem := mt.dataStore.insertOrUpdate(dataElem)
	mt.currSize += dataElem.Size() - oldDataElem.Size()
	if newKeyInserted := oldDataElem == nil; newKeyInserted {
		mt.currCount++
	}
}

func (mt *MemTable) writeAheadLog(dataElem *DataElement) error {
	_, err := mt.walFile.Write(dataElem.Serialization())
	if err != nil {
		return err
	}
	return mt.walFile.Sync()
}

func (mt *MemTable) dataIterate(f func(*DataElement) error) error {
	return mt.dataStore.dataIterate(f)
}

func newDataStore(maxLayer int) *DataStore {
	return &DataStore{
		root: &DataNode{
			next: make([]*DataNode, maxLayer),
		},
		currLayer: 1,
		maxLayer:  maxLayer,
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (ds *DataStore) search(key string) *DataElement {

	currNode := ds.root
	for idx := ds.currLayer - 1; idx >= 0; idx-- {

		for currNode.next[idx] != nil && currNode.next[idx].key <= key {

			if currNode.next[idx].key == key {
				return currNode.next[idx].element
			}

			currNode = currNode.next[idx]
		}
	}

	return nil
}

func (ds *DataStore) insertOrUpdate(elem *DataElement) (oldElem *DataElement) {

	needUpdateNodes := make([]*DataNode, ds.maxLayer)
	currNode := ds.root

	for idx := ds.currLayer - 1; idx >= 0; idx-- {

		for currNode.next[idx] != nil && currNode.next[idx].key <= elem.key {
			if currNode.next[idx].key == elem.key {
				oldElem := currNode.next[idx].element
				currNode.next[idx].element = elem
				return oldElem
			}
			currNode = currNode.next[idx]
		}

		needUpdateNodes[idx] = currNode
	}

	randLayer := ds.r.Intn(ds.maxLayer) + 1
	if randLayer > ds.currLayer {
		for idx := randLayer - 1; idx >= ds.currLayer; idx-- {
			needUpdateNodes[idx] = ds.root
		}
		ds.currLayer = randLayer
	}

	newDataNode := DataNode{
		key:     elem.key,
		element: elem,
		next:    make([]*DataNode, randLayer),
	}

	for idx := randLayer - 1; idx >= 0; idx-- {
		newDataNode.next[idx] = needUpdateNodes[idx].next[idx]
		needUpdateNodes[idx].next[idx] = &newDataNode
	}

	return nil
}

func (ds *DataStore) dataIterate(f func(*DataElement) error) error {

	curr := ds.root
	for curr.next[0] != nil {
		if err := f(curr.next[0].element); err != nil {
			return err
		}
		curr = curr.next[0]
	}

	return nil
}

func (de *DataElement) Serialization() []byte {

	buf := make([]byte, DataElementMetaSize, de.Size())

	if de.isTombstone {
		buf[0] = 1
	}

	keyLen := len(de.key)
	buf[1] = byte(keyLen >> 8)
	buf[2] = byte(keyLen)

	valueLen := len(de.value)
	buf[3] = byte(valueLen >> 24)
	buf[4] = byte(valueLen >> 16)
	buf[5] = byte(valueLen >> 8)
	buf[6] = byte(valueLen)

	buf = append(buf, []byte(de.key)...)
	buf = append(buf, de.value...)
	return buf
}

func (de *DataElement) Deserialization(data []byte) error {

	if len(data) < DataElementMetaSize {
		return ErrIncompleteElement
	}

	de.isTombstone = data[0] == 1

	keyLen := int(binary.BigEndian.Uint16(data[1:3]))
	valueLen := int(binary.BigEndian.Uint32(data[3:7]))

	if len(data) < DataElementMetaSize+keyLen+valueLen {
		return ErrIncompleteElement
	}

	de.key = string(data[DataElementMetaSize : DataElementMetaSize+keyLen])
	value := make([]byte, valueLen)
	copy(value, data[DataElementMetaSize+keyLen:])
	de.value = value
	return nil
}

func (de *DataElement) Size() int {
	if de == nil {
		return 0
	}
	return len(de.key) + len(de.value) + DataElementMetaSize
}

// meta data must be valid
func calcKeyLenValueLenFromMeta(metaData []byte) (int, int) {
	keyLen := int(binary.BigEndian.Uint16(metaData[1:3]))
	valueLen := int(binary.BigEndian.Uint32(metaData[3:7]))
	return keyLen, valueLen
}

func dataElementScannerSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	dataElem := DataElement{}
	if err := dataElem.Deserialization(data); err != nil {
		return 0, nil, nil
	}
	advance = dataElem.Size()
	token = data[:advance]
	return
}
