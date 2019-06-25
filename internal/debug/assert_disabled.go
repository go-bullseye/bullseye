// +build !assert

package debug

// Assert will panic with msg if cond is false.
func Assert(cond bool, msg interface{}) {}