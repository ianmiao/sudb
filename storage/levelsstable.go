package storage

import (
	"sort"
	"sync"
)

type (
	LevelSSTableList struct {
		lock sync.RWMutex
		// a k/v has no more than one snapshot in one level expect for level0 whose elements are pure MemTables,
		// so level0 needs search all elements but not for other levels.
		levelSSTables []*LevelSSTable
	}

	LevelSSTable struct {
		// all fields should be protected
		lock  sync.RWMutex
		level int

		maxSize  int64
		currSize int64
		count    int

		headDummy *SSTable
		tailDummy *SSTable
	}
)

const (
	LevelZero = 0
)

func (lsstl *LevelSSTableList) search(key string) (value []byte, has bool, err error) {
	lsstl.lock.RLock()
	defer lsstl.lock.RUnlock()

	// lower level has newer data
	for _, levelSSTable := range lsstl.levelSSTables {
		value, has, err = levelSSTable.search(key)
		if err != nil {
			return nil, false, err
		}
		if has {
			return value, true, nil
		}
	}

	return nil, false, nil
}

func (lsstl *LevelSSTableList) appendLevel0SSTable(ssTable *SSTable) (needCompaction bool) {
	lsstl.lock.RLock()
	defer lsstl.lock.RUnlock()
	// level 0 must be initialized
	return lsstl.levelSSTables[0].appendSSTable(ssTable)
}

func (lsstl *LevelSSTableList) removeSSTables(ssTables []*SSTable) {
	lsstl.lock.Lock()
	defer lsstl.lock.Unlock()
	for _, ssTable := range ssTables {
		lsstl.levelSSTables[ssTable.level].removeSSTable(ssTable)
	}
}

func (lsstl *LevelSSTableList) insertSSTable(ssTable *SSTable) {
	lsstl.lock.Lock()
	defer lsstl.lock.Unlock()

	if ssTable.level == len(lsstl.levelSSTables) {
		lsstl.levelSSTables = append(lsstl.levelSSTables, newLevelSSTable(ssTable.level, ssTable))
	} else {
		lsstl.levelSSTables[ssTable.level].insertSSTable(ssTable)
	}
}

func (lsstl *LevelSSTableList) chooseCompactionSSTable(level int) *SSTable {
	lsstl.lock.RLock()
	defer lsstl.lock.RUnlock()
	if level >= len(lsstl.levelSSTables) || lsstl.levelSSTables[level].count == 0 {
		return nil
	}
	return lsstl.levelSSTables[level].headDummy.next
}

func (lsstl *LevelSSTableList) currMaxLevel() int {
	lsstl.lock.RLock()
	defer lsstl.lock.RUnlock()
	return len(lsstl.levelSSTables)
}

func (lsstl *LevelSSTableList) searchSSTableRange(level int, minKey string, maxKey string) []*SSTable {
	lsstl.lock.RLock()
	defer lsstl.lock.RUnlock()
	if level >= len(lsstl.levelSSTables) || lsstl.levelSSTables[level].count == 0 {
		return nil
	}

	return lsstl.levelSSTables[level].searchSSTableRange(minKey, maxKey)
}

func newLevelSSTable(level int, ssTables ...*SSTable) *LevelSSTable {
	levelSSTable := LevelSSTable{
		lock:      sync.RWMutex{},
		level:     level,
		maxSize:   calcLevelSSTableMaxSize(level),
		currSize:  0,
		count:     0,
		headDummy: &SSTable{},
		tailDummy: &SSTable{},
	}
	levelSSTable.headDummy.next = levelSSTable.tailDummy
	levelSSTable.tailDummy.prev = levelSSTable.headDummy
	if level == LevelZero {
		// file name is timestamp, older SSTable should be placed earlier
		sort.Slice(ssTables, func(i, j int) bool {
			return ssTables[i].fileName < ssTables[j].fileName
		})
		for _, ssTable := range ssTables {
			levelSSTable.appendSSTable(ssTable)
		}
	} else {
		levelSSTable.insertSSTable(ssTables...)
	}
	return &levelSSTable
}

func (lsst *LevelSSTable) search(key string) ([]byte, bool, error) {
	lsst.lock.RLock()
	defer lsst.lock.RUnlock()

	currSSTable := lsst.tailDummy.prev
	for currSSTable != lsst.headDummy {
		value, has, err := currSSTable.search(key)
		if err != nil {
			return nil, false, err
		}
		if has {
			return value, true, nil
		}
		currSSTable = currSSTable.prev
	}
	return nil, false, nil
}

func (lsst *LevelSSTable) removeSSTable(ssTable *SSTable) {
	lsst.lock.Lock()
	defer lsst.lock.Unlock()

	prevSSTable := ssTable.prev
	nextSSTable := ssTable.next
	prevSSTable.next = nextSSTable
	nextSSTable.prev = prevSSTable
	lsst.currSize -= ssTable.size
	lsst.count--
}

func (lsst *LevelSSTable) appendSSTable(ssTable *SSTable) (needCompaction bool) {
	lsst.lock.Lock()
	defer lsst.lock.Unlock()

	prevTail := lsst.tailDummy.prev
	prevTail.next = ssTable
	ssTable.prev = prevTail
	ssTable.next = lsst.tailDummy
	lsst.tailDummy.prev = ssTable
	lsst.count++
	lsst.currSize += ssTable.size
	return lsst.currSize >= lsst.maxSize
}

// argv has side effect
func (lsst *LevelSSTable) insertSSTable(ssTables ...*SSTable) (needCompaction bool) {
	lsst.lock.Lock()
	defer lsst.lock.Unlock()

	sort.Slice(ssTables, func(i, j int) bool {
		return ssTables[i].minKey < ssTables[j].minKey
	})

	currSSTable := lsst.headDummy
	for _, ssTable := range ssTables {

		lsst.currSize += ssTable.size

		for currSSTable.next != lsst.tailDummy {
			if currSSTable.next.minKey > ssTable.minKey {
				break
			}
			currSSTable = currSSTable.next
		}

		nextSSTable := currSSTable.next
		currSSTable.next = ssTable
		ssTable.prev = currSSTable
		ssTable.next = nextSSTable
		nextSSTable.prev = ssTable
		currSSTable = ssTable
	}

	lsst.count += len(ssTables)
	return lsst.currSize >= lsst.maxSize
}

func (lsst *LevelSSTable) searchSSTableRange(minKey string, maxKey string) []*SSTable {
	lsst.lock.RLock()
	defer lsst.lock.RUnlock()

	find := false
	res := []*SSTable(nil)
	currSSTable := lsst.headDummy.next
	for currSSTable != lsst.tailDummy {
		if !find {
			if currSSTable.minKey >= minKey ||
				(currSSTable.minKey < minKey && currSSTable.maxKey >= minKey) {
				find = true
				res = append(res, currSSTable)
			}
		} else {
			if currSSTable.minKey > maxKey {
				break
			}
			res = append(res, currSSTable)
		}
		currSSTable = currSSTable.next
	}

	return res
}

func calcLevelSSTableMaxSize(level int) int64 {
	maxSize, has := map[int]int64{
		0: 1024 * 1024 * 64,       // 64 MB
		1: 1024 * 1024 * 1024,     // 1 GB
		2: 1024 * 1024 * 1024 * 4, // 4 GB
	}[level]
	if has {
		return maxSize
	}
	return 1024 * 1024 * 1024 * 8
}
