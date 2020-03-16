override SHELL := /usr/bin/env bash -euxo pipefail
override .DEFAULT_GOAL := all

all: force vet lint test

vet: force
	@go vet ./...

lint: force
	@go run golang.org/x/lint/golint -set_exit_status ./...

test: force
	@go test -coverprofile=coverage.txt -covermode=count ./...

.PHONY: force
force:
