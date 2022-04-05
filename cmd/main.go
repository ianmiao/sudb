package main

import (
	"bufio"
	"flag"
	"os"
	"strings"

	"github.com/ianmiao/sudb/storage"
)

func main() {

	dbDir := flag.String("db_dir", "/tmp/sudbtmp", "db dir")
	flag.Parse()

	db, err := storage.NewDB(*dbDir)
	if err != nil {
		panic(err)
	}

	db.Run()

	msgCh := make(chan string)
	go func() {
		for {
			bufReader := bufio.NewReader(os.Stdin)
			msg, err := bufReader.ReadBytes('\n')
			if err != nil {
				panic(err)
			}

			command := strings.Split(strings.Trim(string(msg), "\n"), " ")
			switch command[0] {
			case "insert":

				if err := db.InsertOrUpdate(command[1], []byte(command[2])); err != nil {
					panic(err)
				}
				msgCh <- "ok\n"
			case "delete":

				if err := db.Delete(command[1]); err != nil {
					panic(err)
				}
				msgCh <- "ok\n"
			case "search":

				value, err := db.Search(command[1])
				if err != nil {
					panic(err)
				}
				msgCh <- string(value) + "\n"
			case "exit":
				db.Close()
				msgCh <- "bye"
				close(msgCh)
				return
			}
		}
	}()

	for msg := range msgCh {
		os.Stdout.Write([]byte(msg))
	}
}
