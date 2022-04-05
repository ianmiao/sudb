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
