.PHONY: test lint vet fmt

test:
	go test -v ./...

lint:
	golangci-lint run

vet:
	go vet ./...

fmt:
	gofmt -w .
