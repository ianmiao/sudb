package clog

import (
	"log"
	"os"
)

var (
	DefaultLogger = log.New(os.Stdout, "[SUDB]", log.LstdFlags|log.LUTC|log.Lshortfile)
)
