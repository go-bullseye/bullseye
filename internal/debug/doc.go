/*
Package debug provides compiled assertions, debug and warn level logging.

To enable runtime debug or warn level logging, build with the debug or warn tags
respectively. Building with the debug tag will enable the warn level logger automatically.
When the debug and warn tags are omitted, the code for the logging will be ommitted from
the binary.

To enable runtime assertions, build with the assert tag. When the assert tag is omitted,
the code for the assertions will be ommitted from the binary.
*/
package debug
