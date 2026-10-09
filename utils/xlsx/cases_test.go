package xlsx

import "strings"

// coverage is a workbook holding one cell for each encoding rule, for the
// test that hands the file to LibreOffice.
var coverage = [][]string{
	{"plain", "<a & b>", `say "hi" it's`, " lead", "trail ", "a\tb", "a\nb", "a\r\nb"},
	{"a\x01b", "_x0041_", "_x0041_x0042_", "_x0041\x01", "日本語 😀 é", "=HYPERLINK(\"http://x\",\"y\")", "", "-5"},
	{strings.Repeat("a", MaxCellUnits+5), strings.Repeat("a", MaxCellUnits-1) + "😀", "a\xffb", "x"},
}

// short keeps a failure message readable when a cell is 32,767 units long.
func short(s string) string {
	if len(s) > 40 {
		return s[:20] + "…" + s[len(s)-20:]
	}
	return s
}
