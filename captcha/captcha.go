package captcha

import (
	"bytes"
	"image/color"
	"image/png"
	"math"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/utils/crypto/rand"
	"github.com/fogleman/gg"
)

type ICaptcha interface {
	GenerateImage(string) ([]byte, error)
	GenerateID() (string, string)
}

type Captcha struct {
}

func NewCaptcha() ICaptcha {
	return &Captcha{}
}

func (c *Captcha) GenerateID() (string, string) {
	captchaText := rand.RandomString(6, "abcdefghijklmnopqrstuvwxyzABDELQRTY1234567890")
	captchaID := rand.RandomString(20)

	return captchaID, captchaText
}

func (c *Captcha) GenerateImage(captchaText string) ([]byte, error) {
	const width = 240
	const height = 80
	dc := gg.NewContext(width, height)
	dc.SetRGB(1, 1, 1)
	dc.Clear()

	// Background lines
	dc.SetColor(color.Black)
	for i := 0; i < 15; i++ {
		x1, y1 := rand.Float64()*width, rand.Float64()*height
		x2, y2 := rand.Float64()*width, rand.Float64()*height
		dc.DrawLine(x1, y1, x2, y2)
		dc.Stroke()
	}

	if err := dc.LoadFontFace("./captcha.ttf", 52); err != nil {
		return nil, err
	}

	// Wave parameters
	baselineY := 20.0
	amplitude := 35.0 // Wave height
	frequency := 0.4  // Wave frequency
	startX := 40.0
	spacing := 25.0 // Character spacing

	// Apply uniform wave
	for i, c := range captchaText {
		pos := float64(i)
		// Single wave pattern for both x and y
		wave := math.Sin(frequency * pos)
		x := startX + (pos * spacing) + (amplitude * wave)
		y := baselineY + (amplitude * wave)

		// Add rotation following wave
		angle := wave * 0.8 // Rotate based on wave position
		dc.RotateAbout(angle, x, y)
		dc.DrawStringAnchored(string(c), x, y, 0.5, 0.5)
		dc.RotateAbout(-angle, x, y)
	}

	var img bytes.Buffer
	err := png.Encode(&img, dc.Image())
	if err != nil {
		return nil, err
	}

	return img.Bytes(), nil
}
