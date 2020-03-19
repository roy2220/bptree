#!/usr/bin/env bash

set -euox pipefail

cd $(dirname ${0})
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" .
GOOS=js GOARCH=wasm go build -o bptree.wasm
gzip -9f bptree.wasm
