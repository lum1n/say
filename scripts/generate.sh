#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0 \
  -generate types \
  -package contract \
  -o internal/contract/openapi.gen.go \
  api/openapi.yaml

npm --prefix clients/typescript run generate
