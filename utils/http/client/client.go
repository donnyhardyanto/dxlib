package client

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/log"
)

func HTTPClient(method string, url string, contentType string, body string) (statusCode int, responseBodyAsString string, err error) {
	agent := fiber.AcquireAgent()
	req := agent.Request()
	req.Header.SetMethod(method)
	req.SetRequestURI(url)
	req.Header.SetContentType(contentType)
	req.SetBodyString(body)
	err = agent.Parse()
	if err != nil {
		return 0, "", err
	}
	statusCode, responseBody, errs := agent.Bytes()
	if len(errs) > 0 {
		for _, err := range errs {
			log.Error(err)
		}
		panic(errs)
	}
	return statusCode, string(responseBody), nil
}
