package api

import (
	"context"
	"fmt"
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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsJSON "github.com/donnyhardyanto/dxlib/utils/json"
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
}

type DXAuditLogHandler func(oldAuditLogId int64, parameters *DXAPIAuditLogEntry) (newAuditLogId int64, err error)

type DXAPI struct {
	Version                  string
	NameId                   string
	Address                  string
	WriteTimeoutSec          int
	ReadTimeoutSec           int
	EndPoints                []DXAPIEndPoint
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

func GetIPAddress(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.Header.Get("X-Real-IP")
	}
	if ip == "" {
		ip = r.RemoteAddr
	}
	// Remove port if present
	if strings.Contains(ip, ":") {
		ip, _, _ = net.SplitHostPort(ip)
	}
	return ip
}

func (a *DXAPI) NewEndPoint(title, description, uri, method string, endPointType DXAPIEndPointType,
	contentType utilsHttp.RequestContentType, parameters []DXAPIEndPointParameter, onExecute DXAPIEndPointExecuteFunc,
	onWSLoop DXAPIEndPointExecuteFunc, responsePossibilities map[string]*DXAPIEndPointResponsePossibility, middlewares []DXAPIEndPointExecuteFunc,
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

func (a *DXAPI) routeHandler(w http.ResponseWriter, r *http.Request, p *DXAPIEndPoint) {
	requestContext, span := otel.Tracer(a.Log.Prefix).Start(a.Context, "routeHandler|"+p.Uri)
	defer span.End()

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
		auditLogId, err = a.OnAuditLogStart(auditLogId, &DXAPIAuditLogEntry{
			StartTime: auditLogStartTime,
			IPAddress: GetIPAddress(r),
			APIURL:    r.URL.Path,
			APITitle:  p.Title,
			Method:    r.Method,
		})
	}

	defer func() {
		if a.OnAuditLogEnd != nil {
			_, err = a.OnAuditLogEnd(auditLogId, &DXAPIAuditLogEntry{
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

			// Send HTTP 500 response if not already sent
			if !aepr.ResponseHeaderSent {
				responseBody := utils.JSON{
					"status":         "Internal Server Error",
					"status_code":    http.StatusInternalServerError,
					"reason":         "INTERNAL_SERVER_ERROR",
					"reason_message": "Internal Server Error",
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
				aepr.Log.Infof("%d %s Request: %s", aepr.ResponseStatusCode, r.URL.Path, string(aepr.RequestBodyAsBytes))
			}
		} else {
			aepr.Log.Infof("%d %s", aepr.ResponseStatusCode, r.URL.Path)
		}
	}()

	// TRACE: preprocess_start
	preprocessStartTime := time.Now()
	LogExecutionTrace(requestContext, "preprocess_start", aepr.Id, p.Uri, r.Method, preprocessStartTime, 0, "")

	err = aepr.PreProcessRequest()

	// TRACE: preprocess_end
	if err != nil {
		LogExecutionTrace(requestContext, "preprocess_end", aepr.Id, p.Uri, r.Method, preprocessStartTime, http.StatusBadRequest, err.Error())
	} else {
		LogExecutionTrace(requestContext, "preprocess_end", aepr.Id, p.Uri, r.Method, preprocessStartTime, 0, "")
	}

	if err != nil {
		if aepr.ResponseHeaderSent {
			return
		}
		aepr.WriteResponseAsError(http.StatusBadRequest, err)
		requestDump, err2 := aepr.RequestDumpAsString()
		if err2 != nil {
			aepr.Log.Errorf(err2, "REQUEST_DUMP_ERROR")
			return
		}
		aepr.Log.Errorf(err, "ONPREPROCESSREQUEST_ERROR\nRaw Request:\n%s\n", requestDump)
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

			if aepr.ResponseHeaderSent {
				return
			}
			err3 := errors.Wrap(err, fmt.Sprintf("MIDDLEWARE_ERROR:\n%+v", err))
			aepr.WriteResponseAsError(http.StatusBadRequest, err3)
			requestDump, err2 := aepr.RequestDump()
			if err2 != nil {
				aepr.Log.Errorf(err2, "REQUEST_DUMP_ERROR:%+v", err2)
				return
			}
			aepr.Log.Errorf(err3, "ONMIDDLEWARE_ERROR:%+v\nRaw Request :\n%v\n", err3, string(requestDump))
			return
		}

		LogExecutionTrace(requestContext, fmt.Sprintf("middleware_%d_end", i), aepr.Id, p.Uri, r.Method, middlewareItemStartTime, 0, "")
	}

	// TRACE: middleware_end
	LogExecutionTrace(requestContext, "middleware_end", aepr.Id, p.Uri, r.Method, middlewareStartTime, 0, "")

	aepr.Log.Debugf("Middleware Done: %s", aepr.EndPoint.Uri)

	if aepr.CurrentUser.Id != "" {
		if a.OnAuditLogUserIdentified != nil {
			_, err = a.OnAuditLogUserIdentified(auditLogId, &DXAPIAuditLogEntry{
				StartTime:    auditLogStartTime,
				IPAddress:    GetIPAddress(r),
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

	if p.OnExecute != nil {
		// TRACE: execute_start
		executeStartTime := time.Now()
		LogExecutionTrace(requestContext, "execute_start", aepr.Id, p.Uri, r.Method, executeStartTime, 0, "")

		err = p.OnExecute(aepr)

		if err != nil {
			if aepr.ResponseHeaderSent {
				// TRACE: execute_end (error, response already sent by handler)
				LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, aepr.ResponseStatusCode, err.Error())
				return
			}

			// Check for domain validation errors (e.g., unique field violation)
			// These are expected validation failures, not server errors.
			var domainErr DXAPIDomainError
			if errors.As(err, &domainErr) {
				// TRACE: execute_end (domain validation)
				LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, domainErr.DomainErrorHTTPStatusCode(), domainErr.DomainErrorCode())
				// Log as warning (not error) -- this is expected validation, not a bug
				aepr.Log.Warnf("DOMAIN_VALIDATION:%s:%s", domainErr.DomainErrorCode(), domainErr.DomainErrorLogDetails())
				// Send sanitized response (no DB structure exposed)
				aepr.WriteResponseAsJSON(domainErr.DomainErrorHTTPStatusCode(), nil, domainErr.DomainErrorResponseBody())
				err = nil // clear error so deferred funcs don't treat as error
				return
			}

			// TRACE: execute_end (error)
			LogExecutionTrace(requestContext, "execute_end", aepr.Id, p.Uri, r.Method, executeStartTime, http.StatusBadRequest, err.Error())

			// Log request dump for debugging (before encryption)
			requestDump, err2 := aepr.RequestDump()
			if err2 != nil {
				aepr.Log.Warnf("REQUEST_DUMP_ERROR:%+v", err2)
			} else {
				aepr.Log.Errorf(err, "ONEXECUTE_ERROR:%+v\nRaw Request:\n%s", err, string(requestDump))
			}

			if !aepr.ResponseHeaderSent {
				s := fmt.Sprintf("ONEXECUTE_ERROR:%+v", err)
				_ = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, s, s)
				return
			}
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
	a.HTTPServer = &http.Server{
		Addr:         a.Address,
		Handler:      mux,
		WriteTimeout: time.Duration(a.WriteTimeoutSec) * time.Second,
		ReadTimeout:  time.Duration(a.ReadTimeoutSec) * time.Second,
	}

	// CORS middleware
	corsMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Consider restricting this to specific origins in production
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,PUT,DELETE,PATCH,OPTION")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,X-Var,*")
			w.Header().Set("Access-Control-Expose-Headers", "X-Var")

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

	errorGroup.Go(func() error {
		a.RuntimeIsActive = true
		log.Log.Infof("Listening at %s... start", a.Address)
		err := a.HTTPServer.ListenAndServe()
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

var Manager DXAPIManager

func init() {
	ctx, cancel := context.WithCancel(core.RootContext)
	Manager = DXAPIManager{
		Context: ctx,
		Cancel:  cancel,
		APIs:    map[string]*DXAPI{},
	}
}
