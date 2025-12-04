LINTER_EXE := golangci-lint
LINTER := $(GOPATH)/bin/$(LINTER_EXE)

$(LINTER):
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b $(GOPATH)/bin

.PHONY: lint
lint: $(LINTER)
	$(LINTER) run --fix

.PHONY: clean
clean:
	rm -f kafka-data-keep

# builds our binary
.PHONY: build
build: clean
	CGO_ENABLED=0 go build -o kafka-data-keep -a ./cmd/main.go

.PHONY: test
test:
	go test -v --race -cover ./...

.PHONY: all
all: clean $(LINTER) lint test build
