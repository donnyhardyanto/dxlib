#!/bin/sh
# Writes the excelize fixtures and every golden file in utils/xlsx/testdata.
# excelize is not a dxlib dependency, so generate.go runs in a throwaway
# module outside the repository, at the exact version the goldens record.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
testdata=$(dirname "$here")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp "$here/generate.go" "$work/main.go"
sed -i '' '1,2d' "$work/main.go" 2>/dev/null || sed -i '1,2d' "$work/main.go"
cd "$work"
go mod init goldens >/dev/null 2>&1
go get github.com/xuri/excelize/v2@v2.11.0 >/dev/null 2>&1
go run . "$testdata"
