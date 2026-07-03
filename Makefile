.PHONY: test lint vet fmt

test:
	go test -v -race -tags memebridge_tzdata ./...

lint:
	golangci-lint run

vet:
	go vet ./...

fmt:
	gofmt -s -w .
