package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"time"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
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

// The process-wide outbound TLS state, set once at startup by
// LoadFromConfiguration from the "http-client" configuration's tls block and
// read by every client this package or the api package builds. Both stay nil
// when there is no such block, and a nil Transport on an http.Client is
// http.DefaultTransport -- exactly the client every release before this one
// built -- so a consumer that never writes the block sees no change at all.
//
// Like HTTPClientDoTimeout in the api package, these are assigned before any
// request is made and are not safe to change while requests are in flight.
var (
	// ClientTLS is the parsed block and its reloading certificate; nil when
	// unconfigured or when the block has enabled=false.
	ClientTLS *utilsTLS.DXClientTLS
	transport *http.Transport
)

// LoadFromConfiguration reads the named configuration's "tls" block and builds
// the shared transport. A configuration with no tls block is not an error and
// changes nothing; a tls block with anything wrong in it is fatal, as it is
// for the server side and for the same reason.
func LoadFromConfiguration(configurationNameId string) (err error) {
	configuration, ok := dxlibConfiguration.Manager.Configurations[configurationNameId]
	if !ok {
		return log.Log.FatalAndCreateErrorf("CONFIGURATION_NOT_FOUND:%s", configurationNameId)
	}
	if configuration.Data == nil {
		return nil
	}
	tlsBlock, present := (*configuration.Data)["tls"]
	if !present {
		log.Log.Infof("%s: no tls block; outbound HTTP and WebSocket clients are the bare default", configurationNameId)
		return nil
	}
	tlsKV, ok := tlsBlock.(utils.JSON)
	if !ok {
		return log.Log.FatalAndCreateErrorf("TLS_CONFIG_ERROR:%s.tls:WRONG_TYPE:%T:EXPECTED_OBJECT", configurationNameId, tlsBlock)
	}
	if err = ApplyTLSConfiguration(tlsKV); err != nil {
		return log.Log.FatalAndCreateErrorf("TLS_CONFIG_ERROR:%s.tls/%v", configurationNameId, err)
	}
	return nil
}

// ApplyTLSConfiguration builds the shared transport from one tls block. It is
// what LoadFromConfiguration calls, split out so a test or a program without
// the configuration manager can install a block directly.
func ApplyTLSConfiguration(tlsKV utils.JSON) error {
	c, err := utilsTLS.NewClientTLS(tlsKV)
	if err != nil {
		return err
	}
	if c.Config == nil {
		// enabled=false: validated, and deliberately not in force.
		ClientTLS, transport = nil, nil
		return nil
	}
	// A clone of DefaultTransport, not a bare &http.Transport{}: the bare form
	// would drop the dial and idle timeouts, proxy-from-environment, and --
	// because setting TLSClientConfig disables it unless ForceAttemptHTTP2 is
	// on -- HTTP/2. A process that has replaced DefaultTransport with some
	// other RoundTripper gets an equivalent Transport built by hand.
	var t *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	} else {
		t = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		}
	}
	t.TLSClientConfig = c.Config
	ClientTLS, transport = c, t
	return nil
}

// Transport is the shared outbound transport, or nil when none is configured.
// nil is a valid http.Client.Transport meaning DefaultTransport.
func Transport() http.RoundTripper {
	if transport == nil {
		return nil
	}
	return transport
}

// TLSClientConfig is the shared client tls.Config, or nil when none is
// configured, for a dialer that is not an http.Transport (gorilla/websocket).
func TLSClientConfig() *tls.Config {
	if ClientTLS == nil {
		return nil
	}
	return ClientTLS.Config
}

// NewHTTPClient is the http.Client every outbound call in dxlib should use:
// the shared transport and the given timeout, zero meaning none.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: Transport()}
}

// LogHandshakeFailure logs what a failed outbound dial most likely means when
// TLS is configured: a wrong CA, a clock, a name, a refusal by the far side.
// The error itself is returned to the caller unchanged by every function that
// calls this; the classification is a log line beside it, so nothing a caller
// matches on has moved. l may be nil, in which case the package log is used.
func LogHandshakeFailure(l *log.DXLog, url string, err error) {
	if err == nil || ClientTLS == nil {
		return
	}
	class, advice := utilsTLS.ClassifyHandshakeError(err)
	if class == "" || class == utilsTLS.HandshakeClassOther {
		return
	}
	if l == nil {
		l = &log.Log
	}
	l.Warnf("TLS_CLIENT_HANDSHAKE_FAILED:%s:%s:%v:%s", class, url, err, advice)
}

func httpClientOtelStart(ctx context.Context, method string, url string) (context.Context, func(err error, statusCode int, cs *tls.ConnectionState)) {
	if !core.IsOtelEnabled {
		return ctx, func(error, int, *tls.ConnectionState) { /* no-op: OTel disabled */ }
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
	return ctx, func(err error, statusCode int, cs *tls.ConnectionState) {
		kvs := []attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.Int("http.status_code", statusCode),
		}
		if cs != nil {
			// Negotiated version, suite and the upstream's identity: with these
			// "which upstream is still on 1.2" is a dashboard query.
			kvs = append(kvs,
				attribute.String("tls.version", tls.VersionName(cs.Version)),
				attribute.String("tls.cipher", tls.CipherSuiteName(cs.CipherSuite)),
			)
			if len(cs.PeerCertificates) > 0 {
				kvs = append(kvs, attribute.String("tls.peer_identity", utilsTLS.PeerIdentity(cs.PeerCertificates[0])))
			}
		}
		attrs := metric.WithAttributes(kvs...)
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
	var connState *tls.ConnectionState
	defer func() { endOtel(err, statusCode, connState) }()

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

	// The shared client: bare when no "http-client" tls block is configured,
	// mTLS when one is.
	client := NewHTTPClient(0)
	resp, err := client.Do(request)
	if err != nil {
		LogHandshakeFailure(nil, url, err)
		return nil, nil, err
	}
	statusCode = resp.StatusCode
	connState = resp.TLS
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
