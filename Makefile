.PHONY: fmt lint proto ci tidy

fmt:
@echo "==> running gofmt"
@gofmt -w $(shell find . -name '*.go' -not -path './vendor/*')

lint:
@echo "==> running go vet"
@go vet ./...

proto:
@echo "==> generating protobuf stubs"
@buf generate


ci: fmt lint proto

tidy:
@echo "==> go mod tidy"
@go mod tidy
