GO_BUILD=go build
GO_TEST?=go test
GO_MOD=go mod

GO_SOURCES  := $(shell find . -path -prune -o -name '*.go' -not -name '*_test.go')
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)
GO_TEMPLATES := $(shell find . -path ./vendor -prune -o -name "*.tmpl" -print)
GO_COMPILED_TEMPLATES = $(patsubst %.gen.go.tmpl,%.gen.go,$(GO_TEMPLATES))

default: build

build: vendor go-templates

clean:
	find . -type f -name '*.gen.go' -exec rm {} +
	rm -rf bin/
	rm -rf vendor/

test: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) ./...

ci: test-debug-assert

test-debug-assert: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -tags='debug assert' ./...

bench: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -bench=. -run=- ./...

go-templates: bin/tmpl $(GO_COMPILED_TEMPLATES)

%.gen.go: %.gen.go.tmpl
	bin/tmpl -i -data=numeric.tmpldata $<

fmt: $(SOURCES_NO_VENDOR)
	goimports -w $^

bin/tmpl: ./vendor/github.com/apache/arrow/go/arrow/_tools/tmpl/main.go
	$(GO_BUILD) -o $@ "./$(<D)"

vendor:
	${GO_MOD} vendor

.PHONY: default build clean test ci test-debug-assert bench go-templates