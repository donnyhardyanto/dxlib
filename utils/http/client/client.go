package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"time"

	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type HTTPHeader = map[string]string

type HTTPResponse struct {
	StatusCode int
	Body       []byte
	Headers    map[string][]string
}

func (hr *HTTPResponse) BodyAsString() string {
	return string(hr.Body)
}

func (hr *HTTPResponse) BodyAsJSON() (map[string]any, error) {
	var v map[string]any
	err := json.Unmarshal(hr.Body, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func httpClientOtelStart(ctx context.Context, method string, url string) (context.Context, func(err error, statusCode int)) {
	if !core.IsOtelEnabled {
		return ctx, func(error, int) { /* no-op: OTel disabled */ }
	}
	spanAttrs := []attribute.KeyValue{
		attribute.String("http.method", method),
		attribute.String("http.url", url),
	}
	if parsed, parseErr := neturl.Parse(url); parseErr == nil && parsed.Hostname() != "" {
		spanAttrs = append(spanAttrs, attribute.String("peer.service", parsed.Hostname()))
	}
	ctx, span := otel.Tracer("dxlib.http.client").Start(ctx, "HTTP "+method,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(spanAttrs...),
	)
	start := time.Now()
	return ctx, func(err error, statusCode int) {
		attrs := metric.WithAttributes(
			attribute.String("http.method", method),
			attribute.Int("http.status_code", statusCode),
		)
		dxlibOtel.HTTPClientDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		dxlibOtel.HTTPClientCount.Add(ctx, 1, attrs)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func HTTPClient(ctx context.Context, method string, url string, headers map[string]string, body any) (request *http.Request, response *http.Response, err error) {
	ctx, endOtel := httpClientOtelStart(ctx, method, url)
	statusCode := 0
	defer func() { endOtel(err, statusCode) }()

	var bodyAsBytes []byte
	contentType := ""

	switch body.(type) {
	case string:
		bodyAsBytes = []byte(body.(string))
		break
	case []byte:
		bodyAsBytes = body.([]byte)
		break
	case map[string]any:
		bodyAsBytes, err = json.Marshal(body)
		if err != nil {
			return nil, nil, err
		}
		contentType = "application/json"
		break
	default:
		err = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:TYPE_CANT_BE_CONVERTED_TO_BYTES:%v", body))
		return nil, nil, err
	}

	request, err = http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(bodyAsBytes))
	if err != nil {
		return nil, nil, err
	}

	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	request.Header.Set("Content-Length", fmt.Sprint(len(bodyAsBytes)))

	// Set request headers
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	// RequestCreate an HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	statusCode = resp.StatusCode
	return request, resp, nil
}

func HTTPClientReadAll(ctx context.Context, method string, url string, headers map[string]string, body any) (request *http.Request, response *HTTPResponse, err error) {
	request, resp, err := HTTPClient(ctx, method, url, headers, body)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err2 := resp.Body.Close()
		if err2 != nil {
			slog.Warn("failed to close response body", slog.Any("error", err2))
		}
	}()

	// RequestRead the response body
	responseBodyAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	response = &HTTPResponse{
		StatusCode: resp.StatusCode,
		Body:       responseBodyAsBytes,
		Headers:    resp.Header,
	}
	return request, response, nil
}
