// +build debug

package debug

import (
	"log"
	"os"
)

var (
	debug = log.New(os.Stderr, "(debug) ", log.LstdFlags)
)

func Debug(msg interface{}) {
	debug.Print(msg)
}
