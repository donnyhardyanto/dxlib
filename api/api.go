package api

import (
	"context"
	"fmt"

	"github.com/donnyhardyanto/dxlib"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/pkg/errors"

	"net"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"

	"go.opentelemetry.io/otel"
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
	defer func() {
		if (err != nil) && (dxlib.IsDebug) && (p.RequestContentType == utilsHttp.RequestContentTypeApplicationJSON) {
			if aepr.RequestBodyAsBytes != nil {
				aepr.Log.Infof("%d %s Request: %s", aepr.ResponseStatusCode, r.URL.Path, string(aepr.RequestBodyAsBytes))
			}
		} else {
			aepr.Log.Infof("%d %s", aepr.ResponseStatusCode, r.URL.Path)
		}
	}()

	err = aepr.PreProcessRequest()
	if err != nil {
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

	if aepr.EffectiveRequestHeader == nil {
		aepr.EffectiveRequestHeader = utilsHttp.HeaderToMapStringString(aepr.Request.Header)
	}
	for _, middleware := range p.Middlewares {

		err = middleware(aepr)
		if err != nil {
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

	}

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
		err = p.OnExecute(aepr)
		if err != nil {
			aepr.Log.Errorf(err, "ONEXECUTE_ERROR:\n%+v\n", err)

			requestDump, err2 := aepr.RequestDump()
			if err2 != nil {
				aepr.Log.Errorf(err2, "REQUEST_DUMP_ERROR:%+v", err2)
				return
			}
			aepr.Log.Errorf(err, "ONEXECUTE_ERROR:%+v\nRaw Request :\n%+v\n", err, string(requestDump))

			if !aepr.ResponseHeaderSent {
				s := fmt.Sprintf("ONEXECUTE_ERROR:%+v", err)
				_ = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, s, s)
				return
			}
		} else {
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
