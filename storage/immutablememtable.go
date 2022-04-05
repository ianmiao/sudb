package storage

import "sync"

type (
	// memtables waiting to be serialized
	ImmutableMemTableList struct {
		// business goroutine read, persistence goroutine write
		lock      sync.RWMutex
		count     int
		headDummy *MemTable
		tailDummy *MemTable
	}
)

func newImmutableMemTableList(memTables ...*MemTable) *ImmutableMemTableList {

	memTableList := ImmutableMemTableList{
		lock:      sync.RWMutex{},
		count:     len(memTables),
		headDummy: &MemTable{},
		tailDummy: &MemTable{},
	}

	currTable := memTableList.headDummy
	for _, memTable := range memTables {
		currTable.next = memTable
		memTable.prev = currTable
		currTable = memTable
	}
	currTable.next = memTableList.tailDummy
	memTableList.tailDummy.prev = currTable

	return &memTableList
}

func (mtl *ImmutableMemTableList) appendMemTable(memTable *MemTable) {

	mtl.lock.Lock()
	defer mtl.lock.Unlock()

	prevTail := mtl.tailDummy.prev
	memTable.next = mtl.tailDummy
	mtl.tailDummy.prev = memTable
	prevTail.next = memTable
	memTable.prev = prevTail
	mtl.count++
}

func (mtl *ImmutableMemTableList) search(key string) ([]byte, bool) {

	mtl.lock.RLock()
	defer mtl.lock.RUnlock()

	curr := mtl.tailDummy.prev
	for curr != mtl.headDummy {
		value, has := curr.search(key)
		if has {
			return value, true
		}
		curr = curr.prev
	}

	return nil, false
}

func (mtl *ImmutableMemTableList) top() *MemTable {
	mtl.lock.RLock()
	defer mtl.lock.RUnlock()

	if mtl.headDummy.next == mtl.tailDummy {
		return nil
	}

	return mtl.headDummy.next
}

func (mtl *ImmutableMemTableList) pop() *MemTable {
	mtl.lock.Lock()
	defer mtl.lock.Unlock()

	if mtl.headDummy.next == mtl.tailDummy {
		return nil
	}

	topTable := mtl.headDummy.next
	nextTable := topTable.next
	mtl.headDummy.next = nextTable
	nextTable.prev = mtl.headDummy
	topTable.prev = nil
	topTable.next = nil
	return topTable
}
