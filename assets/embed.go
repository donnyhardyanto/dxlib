package assets

import (
	_ "embed"
)

// ============================================================================
// Embedded Assets - dxlib
// ============================================================================

//go:embed fonts/captcha.ttf
var CaptchaFontBytes []byte
