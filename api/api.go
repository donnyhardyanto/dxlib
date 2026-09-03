package api

import (
	"context"
	"fmt"
	stdlog "log"
	"log/slog"
	"runtime/debug"

	"github.com/donnyhardyanto/dxlib"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/newrelic/go-agent/v3/newrelic"

	"net"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsJSON "github.com/donnyhardyanto/dxlib/utils/json"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
)

const (
	DXAPIDefaultWriteTimeoutSec = 300
	DXAPIDefaultReadTimeoutSec  = 300
)

var UseResponseDataObject = true

// LogExecutionTrace logs execution trace information for Grafana monitoring
// phase: route_start, preprocess_start, preprocess_end, middleware_start, middleware_end, execute_start, execute_end, response_write, route_end
func LogExecutionTrace(ctx context.Context, phase string, requestId string, endpoint string, method string, startTime time.Time, statusCode int, errMsg string) {
	LogExecutionTraceWithStack(ctx, phase, requestId, endpoint, method, startTime, statusCode, errMsg, "")
}

// LogExecutionTraceWithStack logs execution trace information with optional stack trace for Grafana monitoring
func LogExecutionTraceWithStack(ctx context.Context, phase string, requestId string, endpoint string, method string, startTime time.Time, statusCode int, errMsg string, stackTrace string) {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	traceId := spanCtx.TraceID().String()
	spanId := spanCtx.SpanID().String()

	durationMs := float64(time.Since(startTime).Microseconds()) / 1000.0

	attrs := []any{
		slog.String("trace_id", traceId),
		slog.String("span_id", spanId),
		slog.String("request_id", requestId),
		slog.String("phase", phase),
		slog.String("endpoint", endpoint),
		slog.String("method", method),
		slog.Float64("duration_ms", durationMs),
		slog.Int("status_code", statusCode),
	}

	if errMsg != "" {
		attrs = append(attrs, slog.String("error", errMsg))
	}

	if stackTrace != "" {
		attrs = append(attrs, slog.String("stack_trace", stackTrace))
	}

	slog.Info("EXECUTION_TRACE", attrs...)
}

type DXAPIAuditLogEntry struct {
	StartTime    time.Time `json:"start_time,omitempty"`
	EndTime      time.Time `json:"end_time,omitempty"`
	IPAddress    string    `json:"ip_address,omitempty"`
	UserId       string    `json:"user_id,omitempty"`
	UserUid      string    `json:"user_uid,omitempty"`
	UserLoginId  string    `json:"user_loginid,omitempty"`
	UserFullName string    `json:"user_fullname,omitempty"`
	APIURL       string    `json:"api_url,omitempty"`
	APITitle     string    `json:"api_title,omitempty"`
	Method       string    `json:"method,omitempty"`
	StatusCode   int       `json:"status_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	// PeerIdentity is the caller as named by the verified client certificate,
	// when the request arrived over mTLS; empty otherwise. Under direct mTLS
	// there is no proxy in front to set X-Proxy-Client-IP, so IPAddress above
	// is whatever the caller put in X-Forwarded-For and this field is the one
	// that was actually authenticated. See GetIPAddress.
	PeerIdentity string `json:"peer_identity,omitempty"`
}

type DXAuditLogHandler func(ctx context.Context, oldAuditLogId int64, parameters *DXAPIAuditLogEntry) (newAuditLogId int64, err error)

type DXAPI struct {
	Version                      string
	NameId                       string
	Address                      string
	WriteTimeoutSec              int
	ReadTimeoutSec               int
	CORSAllowedOrigins           string // comma-separated allowed origins; empty or "*" = allow all
	EnableBrowserSecurityHeaders bool   // when true, adds X-Content-Type-Options, HSTS, X-Frame-Options
	EndPoints                    []DXAPIEndPoint
	RawHandlers                  []struct {
		Pattern string
		Handler http.Handler
	}
	// TLS is the server's TLS state, built from the "tls" block beside
	// "address" in the api configuration. nil means the block is absent and the
	// listener is plaintext, exactly as before the block existed. A block with
	// enabled=false leaves this non-nil with a nil Config, so the settings can
	// still be reported.
	TLS                      *utilsTLS.DXServerTLS
	RuntimeIsActive          bool
	HTTPServer               *http.Server
	Log                      log.DXLog
	Context                  context.Context
	Cancel                   context.CancelFunc
	OnAuditLogStart          DXAuditLogHandler
	OnAuditLogUserIdentified DXAuditLogHandler
	OnAuditLogEnd            DXAuditLogHandler
}

var SpecFormat = "MarkDown"

func (a *DXAPI) APIHandlerPrintSpec(aepr *DXAPIEndPointRequest) (err error) {
	s, err := a.PrintSpec()
	if err != nil {
		return err
	}
	aepr.WriteResponseAsString(http.StatusOK, nil, s)
	return err
}

func (a *DXAPI) PrintSpec() (s string, err error) {
	s = "# API: " + a.NameId + "\n\n\n"
	s += "## Version " + a.Version + "\n\n"
	for _, v := range a.EndPoints {
		spec, err := v.PrintSpec()
		if err != nil {
			return "", err
		}
		s += spec + "\n"
	}
	return s, nil
}

type DXAPIManager struct {
	Context           context.Context
	Cancel            context.CancelFunc
	APIs              map[string]*DXAPI
	ErrorGroup        *errgroup.Group
	ErrorGroupContext context.Context
}

func (am *DXAPIManager) NewAPI(nameId string) (*DXAPI, error) {
	ctx, cancel := context.WithCancel(am.Context)
	a := DXAPI{
		Version:   "1.0.0",
		NameId:    nameId,
		EndPoints: []DXAPIEndPoint{},
		Context:   ctx,
		Cancel:    cancel,
		Log:       log.NewLog(&log.Log, ctx, nameId),
	}
	am.APIs[nameId] = &a
	return &a, nil
}

func (am *DXAPIManager) LoadFromConfiguration(configurationNameId string) (err error) {
	configuration, ok := dxlibConfiguration.Manager.Configurations[configurationNameId]
	if !ok {
		return log.Log.FatalAndCreateErrorf("configuration '%s' not found", configurationNameId)
	}
	for k, v := range *configuration.Data {
		_, ok := v.(utils.JSON)
		if !ok {
			return log.Log.FatalAndCreateErrorf("Cannot read %s as JSON", k)
		}
		apiObject, err := am.NewAPI(k)
		if err != nil {
			return err
		}
		err = apiObject.ApplyConfigurations(configurationNameId)
		if err != nil {
			return err
		}
	}
	return nil

}
func (am *DXAPIManager) StartAll(errorGroup *errgroup.Group, errorGroupContext context.Context) error {
	am.ErrorGroup = errorGroup
	am.ErrorGroupContext = errorGroupContext

	am.ErrorGroup.Go(func() (err error) {
		<-am.ErrorGroupContext.Done()
		log.Log.Info("API Manager shutting down... start")
		for _, v := range am.APIs {
			vErr := v.StartShutdown()
			if (err == nil) && (vErr != nil) {
				err = vErr
			}
		}
		log.Log.Info("API Manager shutting down... done")
		return nil
	})

	for _, v := range am.APIs {
		err := v.StartAndWait(am.ErrorGroup)
		if err != nil {
			return errors.Wrap(err, "error occurred in StartAndWait()")
		}
	}
	return nil
}

func (am *DXAPIManager) StopAll() (err error) {
	am.ErrorGroupContext.Done()
	err = am.ErrorGroup.Wait()
	if err != nil {
		return errors.Wrap(err, "error occurred in Wait()")
	}
	return nil
}

func (a *DXAPI) ApplyConfigurations(configurationNameId string) (err error) {
	configuration, ok := dxlibConfiguration.Manager.Configurations[configurationNameId]
	if !ok {
		err := log.Log.FatalAndCreateErrorf("CONFIGURATION_NOT_FOUND:%s", configurationNameId)
		return err
	}
	c := *configuration.Data
	c1, ok := c[a.NameId].(utils.JSON)
	if !ok {
		err := log.Log.FatalAndCreateErrorf("CONFIGURATION_NOT_FOUND:%s.%s", configurationNameId, a.NameId)
		return err
	}

	a.Address, ok = c1["address"].(string)
	if !ok {
		err := log.Log.FatalAndCreateErrorf("CONFIGURATION_NOT_FOUND:%s.%s/address", configurationNameId, a.NameId)
		return err
	}
	a.WriteTimeoutSec = utilsJSON.GetNumberWithDefault(c1, "writetimeout-sec", DXAPIDefaultWriteTimeoutSec)
	a.ReadTimeoutSec = utilsJSON.GetNumberWithDefault(c1, "readtimeout-sec", DXAPIDefaultReadTimeoutSec)

	corsOrigins, err := utilsJSON.GetString(c1, "cors-allowed-origins")
	if err == nil {
		a.CORSAllowedOrigins = corsOrigins
	}

	a.EnableBrowserSecurityHeaders = utilsJSON.GetBoolWithDefault(c1, "enable-browser-security-headers", false)

	// The tls block. Absent means plaintext, said out loud so that "no TLS" is
	// a line in the log and not the absence of one. Present means every key in
	// it is validated now, whether or not enabled is true, and any mistake is
	// fatal here, the same way a missing address is: a process that starts
	// with an unresolved trust source and then makes accept/reject decisions
	// is worse than one that never starts.
	//
	// The error from utilsTLS names only the key path and what is wrong with
	// it. It is deliberately not utilsJSON.GetString's text, which prints the
	// whole enclosing map on failure.
	tlsBlock, tlsPresent := c1["tls"]
	if !tlsPresent {
		log.Log.Infof("API %s: TLS disabled (no tls block); listening in plaintext at %s", a.NameId, a.Address)
		return nil
	}
	tlsKV, ok := tlsBlock.(utils.JSON)
	if !ok {
		return log.Log.FatalAndCreateErrorf("TLS_CONFIG_ERROR:%s.%s.tls:WRONG_TYPE:%T:EXPECTED_OBJECT", configurationNameId, a.NameId, tlsBlock)
	}
	a.TLS, err = utilsTLS.NewServerTLS(tlsKV)
	if err != nil {
		return log.Log.FatalAndCreateErrorf("TLS_CONFIG_ERROR:%s.%s.tls/%v", configurationNameId, a.NameId, err)
	}
	switch {
	case a.TLS.Settings.Mode == utilsTLS.ModeHTTP:
		// Plaintext by an explicit word, not by an absent block: info, not a
		// warning, because it is what the operator wrote.
		log.Log.Infof("API %s: TLS mode=http; listening in plaintext at %s", a.NameId, a.Address)
	case a.TLS.Config == nil:
		log.Log.Warnf("API %s: TLS configured but enabled=false; listening in plaintext at %s (%s)", a.NameId, a.Address, a.TLS.Summary())
	}
	return nil
}

func (a *DXAPI) FindEndPointByURI(uri string) *DXAPIEndPoint {
	for _, endPoint := range a.EndPoints {
		if endPoint.Uri == uri {
			return &endPoint
		}
	}
	return nil
}

// GetIPAddress resolves the client IP for audit logging.
//
// BUG-SEC-118: X-Proxy-Client-IP is checked FIRST. api-proxy-v4 resolves the real
// client IP against its configured trusted_proxies and sets that header on the
// upstream request; before this, nothing downstream read it, so the proxy did the
// work and the backend logged the proxy container's own address instead.
//
// Why preferring it is SAFER, not just more accurate: the proxy builds the upstream
// request under a strict DEFAULT-DENY allowlist (setUpstreamHeaders in
// s7-api-proxy/handlers.go - F-PXY-02/F-PXY-NEW-01). Nothing from the client's outer
// headers or decrypted envelope propagates automatically, so a client-supplied
// X-Proxy-Client-IP is DROPPED and cannot survive into the upstream request. The
// header is therefore proxy-owned on any request that came through the proxy, while
// X-Forwarded-For - what this used to trust first - is attacker-settable.
//
// LIMITATION, stated rather than implied: this makes the value trustworthy for
// traffic that arrives THROUGH api-proxy-v4. A service reachable by some other
// ingress could still be sent a forged X-Proxy-Client-IP directly, exactly as it
// could be sent a forged X-Forwarded-For today - so this is an improvement in the
// proxied path and no regression elsewhere, not a blanket guarantee. Closing that
// properly means ensuring the proxy is the only ingress (BUG-SEC-121's territory).
func GetIPAddress(r *http.Request) string {
	ip := r.Header.Get("X-Proxy-Client-IP")
	if ip == "" {
		ip = r.Header.Get("X-Forwarded-For")
	}
	if ip == "" {
		ip = r.Header.Get("X-Real-IP")
	}
	if ip == "" {
		ip = r.RemoteAddr
	}
	// XFF is a comma-separated chain; take the first (original client) entry. The
	// proxy-owned header is always a single address, so this is a no-op for it.
	if i := strings.IndexByte(ip, ','); i >= 0 {
		ip = strings.TrimSpace(ip[:i])
	}
	// Remove port if present
	if strings.Contains(ip, ":") {
		ip, _, _ = net.SplitHostPort(ip)
	}
	return ip
}

func (a *DXAPI) NewEndPoint(title, description, uri, method string, endPointType DXAPIEndPointType,
	contentType utilsHttp.RequestContentType, parameters []DXAPIEndPointParameter, onExecute DXAPIEndPointExecuteFunc,
	onWSLoop DXAPIEndPointExecuteFunc, responsePossibilities *DXAPIEndPointResponsePossibilities, middlewares []DXAPIEndPointExecuteFunc,
	privileges []string, requestMaxContentLength int64, rateLimitGroupNameId string) *DXAPIEndPoint {

	t := a.FindEndPointByURI(uri)
	if t != nil {
		log.Log.Fatalf("Duplicate endpoint uri %s", uri)
	}
	ae := DXAPIEndPoint{
		Owner:                   a,
		Title:                   title,
		Description:             description,
		Uri:                     uri,
		Method:                  method,
		EndPointType:            endPointType,
		RequestContentType:      contentType,
		Parameters:              parameters,
		OnExecute:               onExecute,
		OnWSLoop:                onWSLoop,
		ResponsePossibilities:   responsePossibilities,
		Middlewares:             middlewares,
		Privileges:              privileges,
		RequestMaxContentLength: requestMaxContentLength,
		RateLimitGroupNameId:    rateLimitGroupNameId,
	}
	a.EndPoints = append(a.EndPoints, ae)
	return &ae
}

// NewWSEndPoint registers a WebSocket endpoint served by the library's own
// lifecycle: it opens, reads, writes and closes, and calls back for the parts
// that differ between applications.
//
// onMessage is where an application's protocol lives -- the bytes are never
// inspected here. onPeriodic runs every periodicInterval for anything that has
// to be pushed unasked; with no periodic hook the same tick sends a ping, which
// is what stops an idle connection being reaped in between. A zero interval
// means thirty seconds.
//
// Use NewEndPoint with an onWSLoop instead when an endpoint wants the whole
// lifecycle to itself.
func (a *DXAPI) NewWSEndPoint(title, description, uri, method string,
	onOpen DXAPIEndPointWSOpenFunc, onMessage DXAPIEndPointWSMessageFunc,
	onClose DXAPIEndPointWSCloseFunc, onPeriodic DXAPIEndPointWSPeriodicFunc,
	periodicInterval time.Duration, middlewares []DXAPIEndPointExecuteFunc,
	privileges []string, rateLimitGroupNameId string) *DXAPIEndPoint {

	t := a.FindEndPointByURI(uri)
	if t != nil {
		log.Log.Fatalf("Duplicate endpoint uri %s", uri)
	}
	ae := DXAPIEndPoint{
		Owner:                a,
		Title:                title,
		Description:          description,
		Uri:                  uri,
		Method:               method,
		EndPointType:         EndPointTypeWS,
		RequestContentType:   utilsHttp.RequestContentTypeNone,
		OnWSOpen:             onOpen,
		OnWSMessage:          onMessage,
		OnWSClose:            onClose,
		OnWSPeriodic:         onPeriodic,
		WSPeriodicInterval:   periodicInterval,
		Middlewares:          middlewares,
		Privileges:           privileges,
		RateLimitGroupNameId: rateLimitGroupNameId,
	}
	a.EndPoints = append(a.EndPoints, ae)
	return &ae
}

// dbContextCarrier is a local interface for extracting DB operation context
// from errors without importing databases/db (avoids circular imports).
// Go structural typing means any error implementing these methods matches.
type dbContextCarrier interface {
	DBOperation() string
	DBTableName() string
	DBMaskedDataString() string
}

func (a *DXAPI) routeHandler(w http.ResponseWriter, r *http.Request, p *DXAPIEndPoint) {
	requestContext, span := otel.Tracer(a.Log.Prefix).Start(r.Context(), "routeHandler|"+p.Uri)
	defer span.End()

	if core.IsOtelEnabled {
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.url", r.URL.Path),
			attribute.String("http.route", p.Uri),
		)
	}

	var aepr *DXAPIEndPointRequest
	var err error
	routeStartTime := time.Now()

	defer func() {
		if err != nil {
			//		_ = aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "ERROR_AT_AEPR:%s (%s)", aepr.Id, err)
		}
	}()

	auditLogId := int64(0)
	auditLogStartTime := time.Now()

	if a.OnAuditLogStart != nil {
		auditLogId, err = a.OnAuditLogStart(requestContext, auditLogId, &DXAPIAuditLogEntry{
			StartTime:    auditLogStartTime,
			IPAddress:    GetIPAddress(r),
			PeerIdentity: PeerIdentityFromRequest(r),
			APIURL:       r.URL.Path,
			APITitle:     p.Title,
			Method:       r.Method,
		})
	}

	defer func() {
		if a.OnAuditLogEnd != nil {
			auditCtx, auditCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer auditCancel()
			_, err = a.OnAuditLogEnd(auditCtx, auditLogId, &DXAPIAuditLogEntry{
				StartTime:  auditLogStartTime,
				EndTime:    time.Now(),
				StatusCode: aepr.ResponseStatusCode,
			})
		}
	}()

	aepr = p.NewEndPointRequest(requestContext, w, r)

	// Panic recovery - prevents HTTP connection reset on panic
	defer func() {
		if rec := recover(); rec != nil {
			// Get stack trace
			stackTrace := string(debug.Stack())

			// Format panic message
			panicMsg := fmt.Sprintf("%v", rec)

			// Log to EXECUTION_TRACE with stack trace
			LogExecutionTraceWithStack(requestContext, "panic_recovered", aepr.Id, p.Uri, r.Method, routeStartTime, http.StatusInternalServerError, panicMsg, stackTrace)

			// Log using existing dxlib error mechanism
			panicErr := errors.Errorf("PANIC_RECOVERED: %v", rec)
			requestDump, err2 := aepr.RequestDumpAsString()
			if err2 != nil {
				requestDump = fmt.Sprintf("REQUEST_DUMP_ERROR: %v", err2)
			}
			aepr.Log.Errorf(panicErr, "PANIC_RECOVERED: %v\nStack Trace:\n%s\nRaw Request:\n%s", rec, stackTrace, requestDump)

			// Send HTTP 500 response with error_log reference
			if !aepr.ResponseHeaderSent {
				errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
				responseBody := utils.JSON{
					"status":         "Internal Server Error",
					"status_code":    http.StatusInternalServerError,
					"reason":         "INTERNAL_SERVER_ERROR",
					"reason_message": "INTERNAL_SERVER_ERROR",
					"error_log_ref":  errorLogRef,
				}
				aepr.ResponseStatusCode = http.StatusInternalServerError
				aepr.WriteResponseAsJSON(http.StatusInternalServerError, nil, responseBody)
			}

			// Set error for other defer functions
			err = panicErr
		}
	}()

	// TRACE: route_start
	LogExecutionTrace(requestContext, "route_start", aepr.Id, p.Uri, r.Method, routeStartTime, 0, "")

	defer func() {
		// TRACE: route_end
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		LogExecutionTrace(requestContext, "route_end", aepr.Id, p.Uri, r.Method, routeStartTime, aepr.ResponseStatusCode, errMsg)

		if (err != nil) && (dxlib.IsDebug) && (p.RequestContentType == utilsHttp.RequestContentTypeApplicationJSON) {
			if aepr.RequestBodyAsBytes != nil {
				requestBody := string(aepr.RequestBodyAsBytes)
				const maxLogLength = 2000
				if len(requestBody) > maxLogLength {
					requestBody = requestBody[:maxLogLength] + "... (truncated)"
				}
				aepr.Log.Infof("%d %s Request: %s", aepr.ResponseStatusCode, r.URL.Path, requestBody)
			}
		} else {
			aepr.Log.Infof("%d %s", aepr.ResponseStatusCode, r.URL.Path)
		}

		if core.IsOtelEnabled {
			elapsed := time.Since(routeStartTime).Seconds()
			span.SetAttributes(tlsAttributes(r.TLS, aepr.PeerIdentity)...)
			attrs := metric.WithAttributes(append([]attribute.KeyValue{
				attribute.String("http.method", r.Method),
				attribute.String("http.route", p.Uri),
				attribute.Int("http.status_code", aepr.ResponseStatusCode),
			}, tlsAttributes(r.TLS, aepr.PeerIdentity)...)...)
			dxlibOtel.HTTPRequestDuration.Record(requestContext, elapsed, attrs)
			dxlibOtel.HTTPRequestCount.Add(requestContext, 1, attrs)
			if aepr.ResponseStatusCode >= 500 {
				span.SetStatus(codes.Error, "HTTP 5xx")
			}
		}
	}()

	// WebSocket endpoints bypass PreProcessRequest entirely
	if p.EndPointType == EndPointTypeWS {
		a.handleWebSocket(w, r, aepr)
		return
	}

	// TRACE: preprocess_start
	preprocessStartTime := time.Now()
	LogExecutionTrace(requestContext, "preprocess_start", aepr.Id, p.Uri, r.Method, preprocessStartTime, 0, "")

	// Transport authentication BEFORE any preprocessing (F-BE-02 / BUG-SEC-121). If a host wired
	// OnBeforePreProcessRequest (e.g. bms-common → proxy-JWT validation), it runs here — BEFORE
	// PreProcessRequest triggers the E2EE inner-decrypt / session-bootstrap / replay-check. A returned
	// error rejects the request with 401 and PreProcessRequest is never called, so an unauthenticated
	// caller can never reach the decrypt path. nil hook = no-op (backward compatible).
	if OnBeforePreProcessRequest != nil {
		if authErr := OnBeforePreProcessRequest(aepr); authErr != nil {
			LogExecutionTrace(requestContext, "preprocess_end", aepr.Id, p.Uri, r.Method, preprocessStartTime, http.StatusUnauthorized, authErr.Error())
			if !aepr.ResponseHeaderSent {
				aepr.WriteResponseAsJSON(http.StatusUnauthorized, nil, utils.JSON{
					"status":         "Unauthorized",
					"status_code":    http.StatusUnauthorized,
					"reason":         "PROXY_AUTH_REQUIRED",
					"reason_message": "PROXY_AUTH_REQUIRED",
				})
			}
			return
		}
	}

	err = aepr.PreProcessRequest()

	// TRACE: preprocess_end
	if err != nil {
		LogExecutionTrace(requestContext, "preprocess_end", aepr.Id, p.Uri, r.Method, preprocessStartTime, http.StatusBadRequest, err.Error())
	} else {
		LogExecutionTrace(requestContext, "preprocess_end", aepr.Id, p.Uri, r.Method, preprocessStartTime, 0, "")
	}

	if err != nil {
		// Always log full error + request dump (including decrypted body), even if response already sent
		requestDump, err2 := aepr.RequestDumpAsString()
		if err2 != nil {
			requestDump = "REQUEST_DUMP_ERROR"
		}
		decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
		aepr.Log.Errorf(err, "PREPROCESS_ERROR:%+v\nRaw Request:\n%s%s", err, requestDump, decryptedDump)
		if aepr.ResponseHeaderSent {
			return
		}
		// Send sanitized response with error_log reference for correlation
		errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
		responseBody := utils.JSON{
			"status":         "Bad Request",
			"status_code":    http.StatusBadRequest,
			"reason":         "PREPROCESS_ERROR",
			"reason_message": "PREPROCESS_ERROR",
			"error_log_ref":  errorLogRef,
		}
		aepr.WriteResponseAsJSON(http.StatusBadRequest, nil, responseBody)
		return
	}

	aepr.Log.Debugf("Middleware Start: %s", aepr.EndPoint.Uri)

	// TRACE: middleware_start
	middlewareStartTime := time.Now()
	LogExecutionTrace(requestContext, "middleware_start", aepr.Id, p.Uri, r.Method, middlewareStartTime, 0, "")

	if aepr.EffectiveRequestHeader == nil {
		aepr.EffectiveRequestHeader = utilsHttp.HeaderToMapStringString(aepr.Request.Header)
	}
	for i, middleware := range p.Middlewares {
		middlewareItemStartTime := time.Now()
		LogExecutionTrace(requestContext, fmt.Sprintf("middleware_%d_start", i), aepr.Id, p.Uri, r.Method, middlewareItemStartTime, 0, "")

		err = middleware(aepr)

		if err != nil {
			LogExecutionTrace(requestContext, fmt.Sprintf("middleware_%d_end", i), aepr.Id, p.Uri, r.Method, middlewareItemStartTime, http.StatusBadRequest, err.Error())
			LogExecutionTrace(requestContext, "middleware_end", aepr.Id, p.Uri, r.Method, middlewareStartTime, http.StatusBadRequest, err.Error())

			// Always log full error + request dump (including decrypted body), even if response already sent
			requestDump, err2 := aepr.RequestDumpAsString()
			if err2 != nil {
				requestDump = "REQUEST_DUMP_ERROR"
			}
			decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
			aepr.Log.Errorf(err, "MIDDLEWARE_ERROR:%+v\nRaw Request:\n%s%s", err, requestDump, decryptedDump)
			if aepr.ResponseHeaderSent {
				return
			}
			// Send sanitized response with error_log reference for correlation
			errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
			responseBody := utils.JSON{
				"status":         "Bad Request",
				"status_code":    http.StatusBadRequest,
				"reason":         "MIDDLEWARE_ERROR",
				"reason_message": "MIDDLEWARE_ERROR",
				"error_log_ref":  errorLogRef,
			}
			aepr.WriteResponseAsJSON(http.StatusBadRequest, nil, responseBody)
			return
		}

		LogExecutionTrace(requestContext, fmt.Sprintf("middleware_%d_end", i), aepr.Id, p.Uri, r.Method, middlewareItemStartTime, 0, "")
	}

	// TRACE: middleware_end
	LogExecutionTrace(requestContext, "middleware_end", aepr.Id, p.Uri, r.Method, middlewareStartTime, 0, "")

	aepr.Log.Debugf("Middleware Done: %s", aepr.EndPoint.Uri)

	if aepr.CurrentUser.Id != "" {
		if a.OnAuditLogUserIdentified != nil {
			_, err = a.OnAuditLogUserIdentified(requestContext, auditLogId, &DXAPIAuditLogEntry{
				StartTime:    auditLogStartTime,
				IPAddress:    GetIPAddress(r),
				PeerIdentity: aepr.PeerIdentity,
				APIURL:       r.URL.Path,
				APITitle:     p.Title,
				Method:       r.Method,
				UserId:       aepr.CurrentUser.Id,
				UserUid:      aepr.CurrentUser.Uid,
				UserLoginId:  aepr.CurrentUser.LoginId,
				UserFullName: aepr.CurrentUser.FullName,
			})
		}

	}

	if p.OnExecute != nil && !aepr.ResponseHeaderSent {
		// TRACE: execute_start
		executeStartTime := time.Now()
		LogExecutionTrace(requestContext, "execute_start", aepr.Id, p.Uri, r.Method, executeStartTime, 0, "")

		err = p.OnExecute(aepr)

		if err != nil {
			// Check for domain validation errors (e.g., unique field violation)
			// These are expected validation failures, not server errors.
			var domainErr DXAPIDomainError
			if errors.As(err, &domainErr) {
				// TRACE: execute_end (domain validation)
				LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, domainErr.DomainErrorHTTPStatusCode(), domainErr.DomainErrorCode())
				// Log as warning with full request context for debugging
				requestDump, err2 := aepr.RequestDumpAsString()
				if err2 != nil {
					requestDump = "REQUEST_DUMP_ERROR"
				}
				decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
				domainErrWrapped := errors.Errorf("DOMAIN_VALIDATION:%s:%s", domainErr.DomainErrorCode(), domainErr.DomainErrorLogDetails())
				aepr.Log.LogText(domainErrWrapped, log.DXLogLevelWarn, "", fmt.Sprintf("Raw Request:\n%s%s", requestDump, decryptedDump))
				// Send a sanitized response (no DB structure exposed)
				if !aepr.ResponseHeaderSent {
					aepr.WriteResponseAsJSON(domainErr.DomainErrorHTTPStatusCode(), nil, domainErr.DomainErrorResponseBody())
				}
				err = nil // clear error so deferred functions don't treat as error
				return
			}
			// TRACE: execute_end (error)
			LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, http.StatusInternalServerError, err.Error())

			// Always log full error + request dump (including decrypted body), even if response already sent
			requestDump, err2 := aepr.RequestDumpAsString()
			if err2 != nil {
				requestDump = "REQUEST_DUMP_ERROR"
			}
			// Extract DB operation context if available
			dbContextStr := ""
			var dbCtx dbContextCarrier
			if errors.As(err, &dbCtx) {
				dbContextStr = fmt.Sprintf("\nDB_CONTEXT: %s table=%s data=%s", dbCtx.DBOperation(), dbCtx.DBTableName(), dbCtx.DBMaskedDataString())
			}
			decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
			aepr.Log.Errorf(err, "EXECUTE_ERROR:%+v%s\nRaw Request:\n%s%s", err, dbContextStr, requestDump, decryptedDump)
			// Send sanitized response with error_log reference for correlation
			if !aepr.ResponseHeaderSent {
				errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
				responseBody := utils.JSON{
					"status":         "Internal Server Error",
					"status_code":    http.StatusInternalServerError,
					"reason":         "INTERNAL_SERVER_ERROR",
					"reason_message": "INTERNAL_SERVER_ERROR",
					"error_log_ref":  errorLogRef,
				}
				aepr.WriteResponseAsJSON(http.StatusInternalServerError, nil, responseBody)
			}
			return
		} else {
			// TRACE: execute_end (success)
			LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, aepr.ResponseStatusCode, "")

			if !aepr.ResponseHeaderSent {
				aepr.WriteResponseAsString(http.StatusOK, nil, "")
			}
		}
	}
	return
}

func (a *DXAPI) StartAndWait(errorGroup *errgroup.Group) error {
	if a.RuntimeIsActive {
		return errors.New("SERVER_ALREADY_ACTIVE")
	}

	mux := http.NewServeMux()
	var httpHandler http.Handler = mux
	if core.IsOtelEnabled {
		httpHandler = otelhttp.NewHandler(mux, a.NameId)
	}
	a.HTTPServer = &http.Server{
		Addr:         a.Address,
		Handler:      httpHandler,
		WriteTimeout: time.Duration(a.WriteTimeoutSec) * time.Second,
		ReadTimeout:  time.Duration(a.ReadTimeoutSec) * time.Second,
	}
	tlsEnabled := a.TLS != nil && a.TLS.Config != nil
	if tlsEnabled {
		// ConfigForHTTPServer, not Config: it pins NextProtos to what
		// http.Server negotiates, so the clone handed back after a CA rotation
		// keeps HTTP/2. The WebSocket path is unaffected by HTTP/2 being on --
		// gorilla's dialer and browsers do not offer h2 for an upgrade, and the
		// TLS test in this package proves it against the real listener -- so
		// TLSNextProto is left alone.
		a.HTTPServer.TLSConfig = a.TLS.ConfigForHTTPServer()
		// Every refused handshake is reported through ErrorLog and nowhere
		// else. Left nil, Go writes it to stderr as an unclassified line; this
		// writer sorts it into TRUST, VALIDITY_WINDOW, IDENTITY and so on.
		// Plaintext listeners keep a nil ErrorLog, exactly as before.
		a.HTTPServer.ErrorLog = stdlog.New(utilsTLS.NewHandshakeErrorLogWriter(), "", 0)
	}

	// CORS middleware — parse allowed origins once before the closure
	var allowedOriginsMap map[string]bool
	if a.CORSAllowedOrigins != "" && a.CORSAllowedOrigins != "*" {
		origins := strings.Split(a.CORSAllowedOrigins, ",")
		allowedOriginsMap = make(map[string]bool, len(origins))
		for _, o := range origins {
			allowedOriginsMap[strings.TrimSpace(o)] = true
		}
	}

	corsMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if allowedOriginsMap != nil {
				if origin != "" && allowedOriginsMap[origin] {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
			} else {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,PUT,DELETE,PATCH,OPTION")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,X-Var,*")
			w.Header().Set("Access-Control-Expose-Headers", "X-Var")

			if a.EnableBrowserSecurityHeaders {
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.Header().Set("X-Frame-Options", "DENY")
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			}

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Handler wrapper that adds New Relic if enabled
	wrapHandler := func(handler http.HandlerFunc, name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if core.NewRelicApplication != nil {
				txn := core.NewRelicApplication.StartTransaction(name)
				defer txn.End()

				r = newrelic.RequestWithTransactionContext(r, txn)
				w = txn.SetWebResponse(w)
				handler(w, r)
				return
			}
			// If New Relic is not enabled, just call the handler directly
			handler(w, r)
		}
	}

	// Set up routes
	for _, endpoint := range a.EndPoints {
		p := endpoint
		handlerFunc := func(w http.ResponseWriter, r *http.Request) {
			a.routeHandler(w, r, &p)
		}

		// Always use the wrapper - it will handle both New Relic enabled and disabled cases
		wrappedHandler := wrapHandler(handlerFunc, p.Uri)
		mux.Handle(p.Uri, corsMiddleware(http.HandlerFunc(wrappedHandler)))
	}

	// Register raw handlers (static files, redirects, etc.)
	for _, rh := range a.RawHandlers {
		mux.Handle(rh.Pattern, corsMiddleware(rh.Handler))
	}

	errorGroup.Go(func() error {
		a.RuntimeIsActive = true
		var err error
		if tlsEnabled {
			trust := ""
			if a.TLS.Settings.Mode == utilsTLS.ModeMTLS {
				trust = ", ca-trust=" + a.TLS.Settings.CATrust
			}
			log.Log.Infof("Listening at %s (TLS mode=%s, client-auth=%s%s)... start", a.Address, a.TLS.Settings.Mode, a.TLS.Settings.ClientAuthName, trust)
			// Empty file arguments: the certificate comes from GetCertificate,
			// which is what makes rotation on disk take effect without a
			// restart.
			err = a.HTTPServer.ListenAndServeTLS("", "")
		} else {
			log.Log.Infof("Listening at %s... start", a.Address)
			err = a.HTTPServer.ListenAndServe()
		}
		if (err != nil) && (!errors.Is(err, http.ErrServerClosed)) {
			log.Log.Errorf(err, "HTTP server error: %+v", err)
		}
		a.RuntimeIsActive = false
		log.Log.Infof("Listening at %s... stopped", a.Address)
		return nil
	})

	return nil
}

func (a *DXAPI) StartShutdown() (err error) {
	if a.RuntimeIsActive {
		log.Log.Infof("Shutdown api %s start...", a.NameId)
		err = a.HTTPServer.Shutdown(core.RootContext)
		if err != nil {
			return errors.Wrap(err, "error occurred in HTTPServer.Shutdown()")
		}
		return nil
	}
	return nil
}

// RegisterRawHandler registers a raw http.Handler on the API mux.
// Use this for static file serving, redirects, or other handlers that bypass
// the DXAPIEndPoint middleware pipeline.
func (a *DXAPI) RegisterRawHandler(pattern string, handler http.Handler) {
	a.RawHandlers = append(a.RawHandlers, struct {
		Pattern string
		Handler http.Handler
	}{Pattern: pattern, Handler: handler})
}

var Manager DXAPIManager

func init() {
	ctx, cancel := context.WithCancel(core.RootContext)
	Manager = DXAPIManager{
		Context: ctx,
		Cancel:  cancel,
		APIs:    map[string]*DXAPI{},
	}
}
