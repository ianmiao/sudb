# sudb

## Desc

A simple k/v storage based on LSM Tree with level-based compaction.

Learning purposes, not for production usage.

Inspired by [mdb](https://github.com/alexander-akhmetov/mdb).

## Usage

### Command-line

```
~/ go run cmd/main.go -db_dir=/tmp/sudbtmp
>> insert k1 1
>> ok
>> insert k2 2
>> ok
>> search k1
>> 1
>> insert k1 3
>> ok
>> search k1
>> 3
>> delete k1
>> ok
>> search k1
>>
>> exit
>> bye
```

### Go package

```go
package main

import (
	"fmt"

	"github.com/ianmiao/sudb/storage"
)

func main() {

	db, err := storage.NewDB("/tmp/sudbtmp")
	if err != nil {
		panic(err)
	}

	db.Run()
	defer db.Close()

	if err := db.InsertOrUpdate("k1", []byte{'1'}); err != nil {
		panic(err)
	}

	val, err := db.Search("k1")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", val)

	if err := db.Delete("k1"); err != nil {
		panic(err)
	}
}
```

## Arch

### Components

* MemTable: an in-memory skip list with a write-ahead-log.

* ImmutableMemTables: a list of MemTables waiting to be persisted.

* LevelBasedSSTables: multi-level on-disk files with in-memory sparse index.

### Diagram

![image](https://github.com/ianmiao/sudb/blob/main/docs/sudb_arch.png)


### File format

* MemTable write-ahead-log file format / SSTable data file format

```
[tombstone or not: 1byte][key length: 2byte][value length: 4byte][key][value]
```

* SSTable sparse index file format

```
[offset in data file: 8byte][key length: 2byte][key]
```

## TODO list

1. A well-designed sparse index to support larger storage.
2. A bloom filter to avoid searching	 all SSTables for non-existing k/v.
3. Concurrent read and write.
