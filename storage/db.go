package storage

import (
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ianmiao/sudb/internal/clog"
)

type (
	// LSM Tree implemented
	DB struct {
		// a namespace, all db info here
		dbDir string

		currMemTable *MemTable

		immutableMemTables *ImmutableMemTableList // MemTables need to be transfered into SSTables
		levelSSTables      *LevelSSTableList

		// minor compaction goroutine
		persistenceNotifyChan chan struct{}
		persistenceClosedChan chan struct{}

		// major compaction goroutine
		compactionNotifyChan chan int
		compactionClosedChan chan struct{}

		fileNameLock sync.Mutex // make sure concurrency safety, (mybe use namespace is better)

		logger *log.Logger

		closed int32
	}
)

const (
	CompactionNotifyChanSize = 4
)

const (
	DBClosed = int32(1)
)

const (
	/*
		dir tree:
			/<db_dir>/memtable/wal/               all wal log files

			/<db_dir>/sstable/keyindex/0/         level 0 sstable key index files, file name is same to memtable wal log file
			/<db_dir>/sstable/data/0/             level 0 sstable data files, file name is same to memtable wal log file
			/<db_dir>/sstable/keyindex/1/         level 1 sstable key index files, file name is compaction time
			/<db_dir>/sstable/data/1/             level 1 sstable data files, file name is same to key index file

			/<db_dir>/tmp/keyindex/               tmp key index file when compaction, move it to sstable dir when compaction completed
			/<db_dir>/tmp/data/                   tmp data file when compation
	*/
	MemTableWALPath = "/memtable/wal"

	SSTableKeyIndexPath    = "/sstable/keyindex"
	SSTableDataPath        = "/sstable/data"
	SSTableTmpKeyIndexPath = "/tmp/sstable/keyindex" // store tmp files when compaction
	SSTableTmpDataPath     = "/tmp/sstable/data"
)

func NewDB(dbDir string) (*DB, error) {

	db := DB{
		dbDir: dbDir,

		persistenceNotifyChan: make(chan struct{}, 1),
		persistenceClosedChan: make(chan struct{}),
		compactionNotifyChan:  make(chan int, CompactionNotifyChanSize),
		compactionClosedChan:  make(chan struct{}),

		logger: clog.DefaultLogger,
	}

	if err := mkdirIfNotExist(
		getMemTableWALDir(dbDir),
		getSSTableKeyIndexDir(dbDir), getSSTableDataDir(dbDir),
		getLevelSSTableKeyIndexDir(db.dbDir, LevelZero),
		getLevelSSTableDataDir(db.dbDir, LevelZero),
		getSSTableTmpKeyIndexDir(dbDir),
		getSSTableTmpDataDir(dbDir),
	); err != nil {
		return nil, err
	}

	// tmp file is soft state, just remove it and trigger another process latter
	if err := cleanDirs(
		getSSTableTmpKeyIndexDir(dbDir),
		getSSTableTmpDataDir(dbDir),
	); err != nil {
		return nil, err
	}

	if err := db.restoreLevelSSTables(); err != nil {
		return nil, err
	}

	if err := db.restoreMemTables(); err != nil {
		return nil, err
	}

	return &db, nil
}

func (db *DB) Run() {
	// TODO: add recover
	go db.asyncPersistMemTable() // minor compaction
	go db.asyncCompactSSTable()  // major compaction
}

func (db *DB) Close() {
	if atomic.LoadInt32(&db.closed) == DBClosed {
		return
	}
	close(db.persistenceNotifyChan)
	<-db.persistenceClosedChan
	<-db.compactionClosedChan
	atomic.StoreInt32(&db.closed, DBClosed)
}

func (db *DB) Search(key string) ([]byte, error) {

	// TODO: add bloom filter
	for _, f := range []func(string) ([]byte, bool, error){
		func(key string) ([]byte, bool, error) {
			value, has := db.currMemTable.search(key)
			return value, has, nil
		},
		func(key string) ([]byte, bool, error) {
			value, has := db.immutableMemTables.search(key)
			return value, has, nil
		},
		func(key string) ([]byte, bool, error) {
			return db.levelSSTables.search(key)
		},
	} {
		value, has, err := f(key)
		if err != nil {
			return nil, err
		}

		if has {
			return value, nil
		}
	}

	return nil, nil
}

func (db *DB) InsertOrUpdate(key string, value []byte) error {
	if err := db.currMemTable.insertOrUpdate(key, value); err != nil {
		return err
	}
	return db.persistMemTableIfNeed()
}

func (db *DB) Delete(key string) error {
	if err := db.currMemTable.del(key); err != nil {
		return err
	}
	return db.persistMemTableIfNeed()
}

func (db *DB) persistMemTableIfNeed() error {
	if !db.currMemTable.isFull() {
		return nil
	}

	memTable, err := newMemTable(db, db.newFileName())
	if err != nil {
		return err
	}
	db.immutableMemTables.appendMemTable(db.currMemTable)
	select {
	case db.persistenceNotifyChan <- struct{}{}:
	default:
	}

	db.logger.Printf("curr MemTable [%s] is full, new MemTable [%s]",
		db.currMemTable.fileName, memTable.fileName)

	db.currMemTable = memTable
	return nil
}

func (db *DB) asyncPersistMemTable() {

	for range db.persistenceNotifyChan {
		memTable := db.immutableMemTables.top()
		for memTable != nil {
			ssTable, err := newSSTableFromMemTable(memTable)
			if err != nil {
				panic(err)
				return
			}
			if needCompaction := db.levelSSTables.appendLevel0SSTable(ssTable); needCompaction {
				db.compactionNotifyChan <- LevelZero
			}
			db.immutableMemTables.pop()
			memTable.finalize()

			db.logger.Printf("MemTable [%s] is persisted as level [%d] SSTable", memTable.fileName, LevelZero)

			memTable = db.immutableMemTables.top()
		}
	}
	// notify compaction goroutine to close, and tell db goroutine closed
	close(db.persistenceClosedChan)
}

func (db *DB) asyncCompactSSTable() {

LOOP:
	for {
		select {
		case level := <-db.compactionNotifyChan:
			nextLevel := level + 1
			if err := mkdirIfNotExist(getLevelSSTableKeyIndexDir(db.dbDir, nextLevel),
				getLevelSSTableDataDir(db.dbDir, nextLevel)); err != nil {
				db.logger.Printf("mkdir err[%s]", err.Error())
				break LOOP
			}
			levelSSTable := db.levelSSTables.chooseCompactionSSTable(level)
			if levelSSTable == nil {
				db.logger.Printf("chooseCompactionSSTable find no table to compact")
				continue
			}
			// pick next level's SSTables
			nextLevelSSTables := db.levelSSTables.searchSSTableRange(nextLevel, levelSSTable.minKey,
				levelSSTable.maxKey)
			newSSTableName := db.newFileName()

			db.logger.Printf("compact level [%d] SSTable [%s] and level [%d] SSTables [%v] into level [%d] SSTable [%s]",
				level, levelSSTable.fileName, nextLevel, ssTableNames(nextLevelSSTables...), nextLevel, newSSTableName)

			newSSTable, err := compactSSTables(levelSSTable, nextLevelSSTables, newSSTableName)
			if err != nil {
				db.logger.Printf("compact SSTables err[%s]", err.Error())
				break LOOP
			}
			db.levelSSTables.insertSSTable(newSSTable)
			oldSSTables := append([]*SSTable{levelSSTable}, nextLevelSSTables...)
			db.levelSSTables.removeSSTables(oldSSTables)
			for _, oldSSTable := range oldSSTables {
				oldSSTable.finalize()
			}
		case <-db.persistenceClosedChan:
			break LOOP
		}
	}

	close(db.compactionClosedChan)
}

func (db *DB) restoreMemTables() error {

	memTableWALFileInfos, err := ioutil.ReadDir(getMemTableWALDir(db.dbDir))
	if err != nil {
		return err
	}

	// MemTable may already transfered into level0 SSTable
	// level0 SSTable file name is same to MemTable WAL file
	level0SSTableDataPath := getLevelSSTableDataDir(db.dbDir, 0)
	ssTableDataFileInfos, err := ioutil.ReadDir(level0SSTableDataPath)
	if err != nil {
		return err
	}

	ssTableFileSet := make(map[string]struct{}, len(ssTableDataFileInfos))
	for _, fileInfo := range ssTableDataFileInfos {
		ssTableFileSet[fileInfo.Name()] = struct{}{}
	}

	needRestoreMemTableFileNames := make([]string, 0, len(memTableWALFileInfos))
	for _, fileInfo := range memTableWALFileInfos {
		_, has := ssTableFileSet[fileInfo.Name()]
		if !has {
			needRestoreMemTableFileNames = append(needRestoreMemTableFileNames, fileInfo.Name())
		}
	}

	memTableCnt := len(needRestoreMemTableFileNames)
	if memTableCnt == 0 {
		currMemTable, err := newMemTable(db, db.newFileName())
		if err != nil {
			return err
		}
		db.currMemTable = currMemTable
		db.immutableMemTables = newImmutableMemTableList()
		return nil
	}

	memTables := make([]*MemTable, memTableCnt)

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(memTableCnt)

	for i, fileName := range needRestoreMemTableFileNames {

		go func(idx int, fileName string) {
			defer wg.Done()
			memTable, err := newMemTable(db, fileName)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err := memTable.restoreFromWAL(); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			memTables[idx] = memTable
		}(i, fileName)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}

	sort.Slice(memTables, func(i, j int) bool {
		return memTables[i].fileName < memTables[j].fileName
	})

	// since all MemTable's file name is timestamp, the bigest one is curr MemTable
	db.currMemTable = memTables[memTableCnt-1]
	db.immutableMemTables = newImmutableMemTableList(memTables[:memTableCnt-1]...)
	return nil
}

func (db *DB) restoreLevelSSTables() error {

	// data files and index files are one-to-one, and have same file name
	levelSSTableDataDirs, err := ioutil.ReadDir(getSSTableDataDir(db.dbDir))
	if err != nil {
		return err
	}

	levelCnt := len(levelSSTableDataDirs)
	levelSSTableList := LevelSSTableList{
		levelSSTables: make([]*LevelSSTable, levelCnt),
	}

	// TODO: load file concurrently
	for _, levelSSTableDir := range levelSSTableDataDirs {

		level, err := strconv.Atoi(levelSSTableDir.Name())
		if err != nil {
			return err
		}

		ssTableDataFiles, err := ioutil.ReadDir(getLevelSSTableDataDir(db.dbDir, level))
		if err != nil {
			return err
		}

		ssTables := make([]*SSTable, len(ssTableDataFiles))
		for idx, ssTableDataFile := range ssTableDataFiles {
			ssTable, err := restoreSSTable(db, level, ssTableDataFile.Name())
			if err != nil {
				return err
			}
			ssTables[idx] = ssTable
		}
		levelSSTableList.levelSSTables[level] = newLevelSSTable(level, ssTables...)
	}

	db.levelSSTables = &levelSSTableList
	return nil
}

func (db *DB) newFileName() string {
	db.fileNameLock.Lock()
	defer db.fileNameLock.Unlock()
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
