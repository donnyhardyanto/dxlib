package oam

import (
	v1 "dxlib/v3/api"
	"github.com/gofiber/fiber/v2"
)

func LogRequest(r *fiber.Ctx) (map[string]interface{}, error) {
	requestBodyAsBytes := r.Body()
	lr := map[string]interface{}{
		"remote_addr": r.IP(),
		"method":      r.Method(),
		"host":        r.Hostname(),
		"request_uri": r.OriginalURL(),
		"query":       r.Queries(),
		"header":      r.Request().Header.String(),
		"body":        requestBodyAsBytes,
	}
	return lr, nil
}

func Echo(r *fiber.Ctx) (map[string]interface{}, error) {
	lr, err := LogRequest(r)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		`log_request`: lr,
	}
	return data, nil
}

func Ping(aepr *v1.DXAPIEndPointRequest) (err error) {
	data, err := Echo(aepr.FiberContext)
	aepr.Log.Infof("Receive (%v): %v", err, data)
	err = aepr.ResponseSetFromJSON(data)
	return err
}
