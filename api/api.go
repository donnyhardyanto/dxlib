package api

import (
	"context"
	"errors"
	"github.com/donnyhardyanto/dxlib"
	"net/http"
	"time"

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

type DXAPI struct {
	NameId          string
	Address         string
	WriteTimeoutSec int
	ReadTimeoutSec  int
	EndPoints       []DXAPIEndPoint
	RuntimeIsActive bool
	HTTPServer      *http.Server
	Log             log.DXLog
	Context         context.Context
	Cancel          context.CancelFunc
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
		log.Log.Info(`API Manager shutting down... start`)
		for _, v := range am.APIs {
			vErr := v.StartShutdown()
			if (err == nil) && (vErr != nil) {
				err = vErr
			}
		}
		log.Log.Info(`API Manager shutting down... done`)
		return nil
	})

	for _, v := range am.APIs {
		err := v.StartAndWait(am.ErrorGroup)
		if err != nil {
			return err
		}
	}
	return nil
}

func (am *DXAPIManager) StopAll() (err error) {
	am.ErrorGroupContext.Done()
	err = am.ErrorGroup.Wait()
	return err
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

	a.Address, ok = c1[`address`].(string)
	if !ok {
		err := log.Log.FatalAndCreateErrorf("CONFIGURATION_NOT_FOUND:%s.%s/address", configurationNameId, a.NameId)
		return err
	}
	a.WriteTimeoutSec = utilsJSON.GetNumberWithDefault(c1, `writetimeout-sec`, DXAPIDefaultWriteTimeoutSec)
	a.ReadTimeoutSec = utilsJSON.GetNumberWithDefault(c1, `readtimeout-sec`, DXAPIDefaultReadTimeoutSec)
	return err
}

func (a *DXAPI) FindEndPointByURI(uri string) *DXAPIEndPoint {
	for _, endPoint := range a.EndPoints {
		if endPoint.Uri == uri {
			return &endPoint
		}
	}
	return nil
}

func (a *DXAPI) NewEndPoint(title, description, uri, method string, endPointType DXAPIEndPointType,
	contentType utilsHttp.RequestContentType, parameters []DXAPIEndPointParameter, onExecute DXAPIEndPointExecuteFunc,
	onWSLoop DXAPIEndPointExecuteFunc, responsePossibilities map[string]*DXAPIEndPointResponsePossibility, middlewares []DXAPIEndPointExecuteFunc) *DXAPIEndPoint {

	t := a.FindEndPointByURI(uri)
	if t != nil {
		log.Log.Fatalf("Duplicate endpoint uri %s", uri)
	}
	ae := DXAPIEndPoint{
		Owner:                 a,
		Title:                 title,
		Description:           description,
		Uri:                   uri,
		Method:                method,
		EndPointType:          endPointType,
		RequestContentType:    contentType,
		Parameters:            parameters,
		OnExecute:             onExecute,
		OnWSLoop:              onWSLoop,
		ResponsePossibilities: responsePossibilities,
		Middlewares:           middlewares,
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

	aepr = p.NewEndPointRequest(requestContext, w, r)
	defer func() {
		if (err != nil) && (dxlib.IsDebug) && (p.RequestContentType == utilsHttp.ContentTypeApplicationJSON) {
			if aepr.RequestBodyAsBytes != nil {
				aepr.Log.Infof("%d %s Request: %s", aepr._responseStatusCode, r.URL.Path, string(aepr.RequestBodyAsBytes))
			}
		} else {
			aepr.Log.Infof("%d %s", aepr._responseStatusCode, r.URL.Path)
		}
	}()

	err = aepr.PreProcessRequest()
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "PREPROCESS_REQUEST_ERROR:%v ", err.Error())
		return
	}

	for _, middleware := range p.Middlewares {
		err = middleware(aepr)
		if err != nil {
			err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "MIDDLEWARE_ERROR:%v ", err.Error())
			return
		}
	}

	if p.OnExecute != nil {
		err = p.OnExecute(aepr)
		if err != nil {
			if !aepr.ResponseHeaderSent {
				err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "ONEXECUTE_ERROR:%v", err.Error())
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
		return errors.New("server is already active")
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
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,*")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Set up routes
	for _, endpoint := range a.EndPoints {
		p := endpoint
		mux.Handle(p.Uri, corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a.routeHandler(w, r, &p)
		})))
	}

	errorGroup.Go(func() error {
		a.RuntimeIsActive = true
		log.Log.Infof("Listening at %s... start", a.Address)
		err := a.HTTPServer.ListenAndServe()
		if (err != nil) && (!errors.Is(err, http.ErrServerClosed)) {
			log.Log.Errorf("HTTP server error: %v", err.Error())
		}
		a.RuntimeIsActive = false
		log.Log.Infof("Listening at %s... stopped", a.Address)
		return err
	})

	return nil
}

func (a *DXAPI) StartShutdown() (err error) {
	if a.RuntimeIsActive {
		log.Log.Infof("Shutdown api %s start...", a.NameId)
		err = a.HTTPServer.Shutdown(core.RootContext)
		return err
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
