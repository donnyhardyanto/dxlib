package captcha

import (
	"bytes"
	"image/color"
	"image/png"
	"math"
	"math/rand"
	"time"

	"github.com/fogleman/gg"
)

type ICaptcha interface {
	GenerateImage(string) ([]byte, error)
	GenerateID() (string, string)
}

type Captcha struct {
}

// Inisialisasi random seed
func init() {
	rand.NewSource(time.Now().UnixNano())
}

func NewCaptcha() ICaptcha {
	return &Captcha{}
}

func (c *Captcha) GenerateID() (string, string) {
	captchaText := generateRandomString(6)
	captchaID := generateRandomString(20)

	return captchaID, captchaText
}

func generateRandomString(length int) string {
	//	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	letters := []rune("abcdefghijklmnopqrstuvwxyzABDELQRTY1234567890")

	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func (c *Captcha) GenerateImage(captchaText string) ([]byte, error) {
	// Generate gambar captcha menggunakan gg (Graphics Library)
	const width = 240
	const height = 80
	dc := gg.NewContext(width, height)
	dc.SetRGB(1, 1, 1)
	dc.Clear()

	// Add noise (random lines) ke background
	dc.SetColor(color.Black)
	for i := 0; i < 15; i++ {
		x1, y1 := rand.Float64()*width, rand.Float64()*height
		x2, y2 := rand.Float64()*width, rand.Float64()*height
		xr, yr := rand.Float64()*10, rand.Float64()*10
		dc.DrawLine(x1, y1, x2, y2)
		dc.DrawLine(x1+xr, y1+yr, x2, y2)
		dc.Stroke()
	}

	shearX := 0.3 // Try values between 0.1 and 0.5
	shearY := 0.0 // Usually keep Y shear at 0 for readable text
	dc.Shear(shearX, shearY)

	// Add noise (random dots) ke background
	for i := 0; i < 300; i++ {
		x, y, r := rand.Float64()*width, rand.Float64()*height, rand.Float64()*2.5
		dc.DrawCircle(x, y, r)
		dc.Fill()
	}

	if err := dc.LoadFontFace("./captcha.ttf", 52); err != nil {
		return nil, err
	}

	startX := 70.0    // Awal posisi X untuk karakter pertama
	yPosition := 40.0 // Posisi Y tetap sama untuk semua karakter

	for i, c := range captchaText {

		dx := math.Sin(float64(i)/2.0) * 5  // Distorsi wave horizontal
		dy := math.Cos(float64(i)/3.0) * 10 // Distorsi wave vertical
		// Rotate karakter lebih besar, antara -25 hingga +25 derajat
		angle := (rand.Float64()*50 - 25) * (math.Pi / 180) // Konversi derajat ke radian

		// Set rotasi dan posisi huruf dengan efek berdempetan
		dc.RotateAbout(angle, startX+dx, yPosition+dy)
		dc.DrawStringAnchored(string(c), startX+dx, yPosition+dy, 0.5, 0.5)
		dc.RotateAbout(-angle, startX+dx, yPosition+dy) // Rotate kembali

		// Geser startX untuk huruf berikutnya, dengan jarak yang lebih kecil (huruf lebih dempet)
		startX += 20 // Jarak antar huruf lebih kecil dari ukuran font agar huruf tampak berdempetan
	}
	dc.Stroke()
	sx, sy := rand.Float64()*width, rand.Float64()*height
	dc.Shear(startX+sx, yPosition+sy)
	var img bytes.Buffer
	err := png.Encode(&img, dc.Image())
	if err != nil {
		return nil, err
	}

	return img.Bytes(), nil
}
