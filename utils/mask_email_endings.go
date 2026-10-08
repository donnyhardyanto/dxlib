package utils

// MaskEmailKnownEndings is the list of public domain endings MaskEmail2by2 keeps as written.
// Everything before the ending is masked, so a sub-domain such as an employer's name in
// "mail.example.co.id" is not shown. The longest ending a domain matches wins, so ".co.id" is
// taken over ".id". Each entry starts with a dot. A host may append to it at init.
var MaskEmailKnownEndings = []string{
	// Indonesia
	".co.id", ".ac.id", ".go.id", ".or.id", ".web.id", ".sch.id", ".net.id", ".my.id",
	".mil.id", ".biz.id", ".ponpes.id", ".desa.id", ".id",

	// Generic
	".com", ".net", ".org", ".edu", ".gov", ".mil", ".int", ".info", ".biz",
	".io", ".co", ".me", ".app", ".dev",

	// Regions
	".co.uk", ".ac.uk", ".org.uk", ".uk",
	".com.sg", ".edu.sg", ".sg",
	".com.my", ".edu.my", ".my",
	".com.au", ".edu.au", ".au",
	".co.jp", ".ac.jp", ".jp",
}
