// +build !debug,!warn

package debug

func Warn(interface{}) {}

func Warnf(format string, v ...interface{}) {}
