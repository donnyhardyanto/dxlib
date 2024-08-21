package minio

import (
	"dxlib/v3/utils"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/teris-io/shortid"
	"io"
)

func Upload(minioConfig utils.JSON, c *fiber.Ctx) error {
	minioClient, err := minio.New(
		minioConfig["address"].(string),
		&minio.Options{
			Creds: credentials.NewStaticV4(
				minioConfig["user_name"].(string),
				minioConfig["user_password"].(string),
				""),
			Secure: minioConfig["use_ssl"].(bool),
		})
	if err != nil {
		return err
	}
	if minioConfig["max_file_size_bytes"] == nil {
		minioConfig["max_file_size_bytes"] = 31 << 26
	}
	c.Context().Request.SetBodyStream(c.Context().Request.BodyStream(), minioConfig["max_file_size_bytes"].(int))

	// Parse the multipart form
	form, err := c.MultipartForm()
	if err != nil {
		return err
	}

	files := form.File["file"]
	if len(files) == 0 {
		return err
	}

	file := files[0]
	fileHeader, err := file.Open()
	if err != nil {
		return err
	}
	defer fileHeader.Close()

	pr, pw := io.Pipe()

	// Stream the file to Minio
	go func() {
		defer pw.Close()
		_, err := io.Copy(pw, fileHeader)
		if err != nil {
			l.Error("Error copying file to pipe:%v", err)
			return
		}
	}()

	filenameTemplate := minioConfig["filename_template"].(string)
	objectName := fmt.Sprintf("%s_%s_%s", filenameTemplate, file.Filename, shortid.MustGenerate())
	bucketName := minioConfig["bucket_name"]
	contentType := file.Header.Get("Content-Type")

	// Create a new object in Minio
	_, err = minioClient.PutObject(context.Background(), bucketName, objectName, pr, -1, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Unable to upload file to Minio")
	}

	return c.SendString("File uploaded successfully")
}
