package xlsx

import "strings"

// coverage is a workbook holding one cell for each encoding rule, for the
// tests that hand the file to another reader.
var coverage = [][]string{
	{"plain", "<a & b>", `say "hi" it's`, " lead", "trail ", "a\tb", "a\nb", "a\r\nb"},
	{"a\x01b", "_x0041_", "_x0041_x0042_", "_x0041\x01", "日本語 😀 é", "=HYPERLINK(\"http://x\",\"y\")", "", "-5"},
	{strings.Repeat("a", MaxCellUnits+5), strings.Repeat("a", MaxCellUnits-1) + "😀", "a\xffb", "x"},
}
