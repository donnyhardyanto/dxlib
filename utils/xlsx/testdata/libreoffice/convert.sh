#!/bin/sh
# Converts source.fods to ../libreoffice.xlsx with LibreOffice, in a profile of
# its own so another running instance does not interfere.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
bin=$(command -v soffice || echo /Applications/LibreOffice.app/Contents/MacOS/soffice)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
"$bin" -env:UserInstallation="file://$work/profile" --headless --convert-to xlsx:"Calc MS Excel 2007 XML" --outdir "$work" "$here/source.fods" >/dev/null
cp "$work/source.xlsx" "$here/../libreoffice.xlsx"
"$bin" --version
