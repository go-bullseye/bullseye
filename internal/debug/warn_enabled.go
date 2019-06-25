// +build debug warn

package debug

import (
	"log"
	"os"
)

var (
	warn = log.New(os.Stderr, "(warn) ", log.LstdFlags)
)

func Warn(msg interface{}) {
	warn.Print(msg)
}

func Warnf(format string, v ...interface{}) {
	warn.Printf(format, v...)
}
