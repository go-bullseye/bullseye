module github.com/go-bullseye/bullseye

go 1.12

require (
	github.com/apache/arrow/go/arrow v0.0.0-20190920001900-00a3c47b1559
	github.com/pkg/errors v0.8.1
)

replace github.com/apache/arrow/go/arrow => ../arrow/go/arrow
