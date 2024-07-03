package api

import (
	"context"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"dxlib/v3/configurations"
	"dxlib/v3/core"
	"dxlib/v3/log"
	"dxlib/v3/utils"
	utilsHttp "dxlib/v3/utils/http"
	"dxlib/v3/utils/json"
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
	HTTPServer      *fiber.App
	Log             log.DXLog
	Context         context.Context
	Cancel          context.CancelFunc
	IsDebug         bool
}

var SpecFormat = "MarkDown"

func (a *DXAPI) APIHandlerPrintSpec(aepr *DXAPIEndPointRequest) (err error) {
	aepr.FiberContext.Response().Header.SetContentType("text/markdown")
	aepr.FiberContext.SendString(a.PrintSpec())
	return nil
}

func (a *DXAPI) PrintSpec() (s string) {
	s = "# API: " + a.NameId + "\n\n\n"
	for _, v := range a.EndPoints {
		s += v.PrintSpec() + "\n"
	}
	return s
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

func (a *DXAPI) ApplyConfigurations() (err error) {
	configuration, ok := configurations.Manager.Configurations["api"]
	if !ok {
		err := log.Log.FatalAndCreateErrorf("Can not find configuration 'api' needed to configure the API")
		return err
	}
	c := *configuration.Data
	c1, ok := c[a.NameId].(utils.JSON)
	if !ok {
		err := log.Log.FatalAndCreateErrorf("Can not find configuration 'api.%s' needed to configure the API", a.NameId)
		return err
	}

	a.Address, ok = c1[`address`].(string)
	if !ok {
		err := log.Log.FatalAndCreateErrorf("Can not find configuration 'api.%s/address' needed to configure the API", a.NameId)
		return err
	}
	a.WriteTimeoutSec = json.GetNumberWithDefault(c1, `writetimeout-sec`, DXAPIDefaultWriteTimeoutSec)
	a.ReadTimeoutSec = json.GetNumberWithDefault(c1, `readtimeout-sec`, DXAPIDefaultReadTimeoutSec)
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
	onWSLoop DXAPIEndPointExecuteFunc, responsePossibilities map[string]*DxAPIEndPointResponsePossibility) *DXAPIEndPoint {

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
	}
	a.EndPoints = append(a.EndPoints, ae)
	return &ae
}

func (a *DXAPI) StartAndWait(errorGroup *errgroup.Group) error {
	if !a.RuntimeIsActive {
		err := a.ApplyConfigurations()
		if err != nil {
			return err
		}
		a.HTTPServer = fiber.New(fiber.Config{
			ReadTimeout:  time.Duration(a.ReadTimeoutSec) * time.Second,
			WriteTimeout: time.Duration(a.WriteTimeoutSec) * time.Second,
		})
		a.HTTPServer.Use(cors.New(cors.Config{
			AllowOrigins: "*",                                   // Allows all origins
			AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH",      // Specify what methods to allow
			AllowHeaders: "Origin, Content-Type, Accept, x-ijt", // Specify what headers can be sent
		}))
		for _, v := range a.EndPoints {
			p := v
			if p.EndPointType == EndPointTypeHTTP {
				a.HTTPServer.Add(p.Method, p.Uri, func(c *fiber.Ctx) error {
					var aepr *DXAPIEndPointRequest
					var err error
					defer func() {
						if err != nil {
							if aepr.ResponseStatusCode == http.StatusOK {
								aepr.ResponseStatusCode = http.StatusInternalServerError
							}
							aepr.Log.Errorf("Error at %s (%s) ", aepr.Id, err)

						} else {
							if aepr.ResponseStatusCode < 300 {
								x := aepr.FiberContext.Response().Header
								y := x.ContentType()
								if y == nil {
									x.Set(`Content-Type`, `application/octet; charset=utf-8`)
								}
							}
						}
						contentLengthBytes := len(aepr.ResponseBodyAsBytes)
						contentLengthBytesAsString := strconv.FormatInt(int64(contentLengthBytes), 10)
						aepr.FiberContext.Response().Header.Set(`Content-Length`, contentLengthBytesAsString)
						aepr.FiberContext.Response().SetStatusCode(aepr.ResponseStatusCode)

						if aepr.ResponseBodyAsBytes != nil {
							errWrite := aepr.FiberContext.Send(aepr.ResponseBodyAsBytes)
							if errWrite != nil {
								aepr.Log.Errorf("DXAPIEndPoint/DXAPIEndPoint/aepr.FiiberContext.Send (%v), reply-data: %v", errWrite, aepr.FiberContext.Response().Body())
								aepr.ResponseErrorAsString = errWrite.Error()
							}
						}
					}()
					requestContext, span := otel.Tracer(a.Log.Prefix).Start(a.Context, "RequestHandler|"+p.Uri)
					defer span.End()

					aepr = p.NewEndPointRequest(requestContext, c)
					defer func() {
						aepr.Log.Infof("%d %s %s", aepr.ResponseStatusCode, aepr.ResponseErrorAsString, aepr.FiberContext.OriginalURL())
					}()

					err = aepr.PreProcessRequest()
					if err != nil {
						aepr.Log.Errorf("Error at PreProcessRequest (%s) ", err)
						aepr.ResponseStatusCode = 422
						return nil
					}

					if p.OnExecute != nil {
						err = p.OnExecute(aepr)
						if err != nil {
							aepr.Log.Errorf("Error at OnExecute (%s) ", err)
							if aepr.ResponseStatusCode == 200 {
								aepr.ResponseStatusCode = 500
							}
							return nil
						}
					}
					return nil
				})
			}
			if p.EndPointType == EndPointTypeWS {
				a.HTTPServer.Add(p.Method, p.Uri, func(c *fiber.Ctx) error {
					var aepr *DXAPIEndPointRequest
					var err error
					defer func() {
						if err != nil {
							if aepr.ResponseStatusCode == http.StatusOK {
								aepr.ResponseStatusCode = http.StatusInternalServerError
							}
							aepr.Log.Errorf("Error at %s (%s) ", aepr.Id, err)
							contentLengthBytes := len(aepr.ResponseBodyAsBytes)
							contentLengthBytesAsString := strconv.FormatInt(int64(contentLengthBytes), 10)
							aepr.FiberContext.Response().Header.Set(`Content-Length`, contentLengthBytesAsString)
							aepr.FiberContext.Response().SetStatusCode(aepr.ResponseStatusCode)

							if aepr.ResponseBodyAsBytes != nil {
								errWrite := aepr.FiberContext.Send(aepr.ResponseBodyAsBytes)
								if errWrite != nil {
									aepr.Log.Errorf("DXAPIEndPoint/DXAPIEndPoint/aepr.FiiberContext.Send (%v), reply-data: %v", errWrite, aepr.FiberContext.Response().Body())
									aepr.ResponseErrorAsString = errWrite.Error()
								}
							}
						}
					}()
					requestContext, span := otel.Tracer(a.Log.Prefix).Start(a.Context, "RequestHandler|"+p.Uri)
					defer span.End()

					aepr = p.NewEndPointRequest(requestContext, c)
					defer func() {
						aepr.Log.Infof("%d %s %s", aepr.ResponseStatusCode, aepr.ResponseErrorAsString, aepr.FiberContext.OriginalURL())
					}()

					err = aepr.PreProcessRequest()
					if err != nil {
						aepr.Log.Errorf("Error at PreProcessRequest (%s) ", err)
						aepr.ResponseStatusCode = 422
						return nil
					}

					if p.OnExecute != nil {
						err = p.OnExecute(aepr)
						if err != nil {
							aepr.Log.Errorf("Error at OnExecute (%s) ", err)
							if aepr.ResponseStatusCode == 200 {
								aepr.ResponseStatusCode = 500
							}
							return nil
						}
					}
					return c.Next()
				}, websocket.New(func(c *websocket.Conn) {
					var aepr *DXAPIEndPointRequest
					if p.OnWSLoop != nil {
						aepr.WSConnection = c
						err := p.OnWSLoop(aepr)
						if err != nil {
							return
						}
					}

				}))
			}

		}

		/*a.RuntimeServer = &http.Server{
			Handler:      r,
			Addr:         a.Address,
			WriteTimeout: time.Duration(a.WriteTimeoutSec) * time.Second,
			ReadTimeout:  time.Duration(a.ReadTimeoutSec) * time.Second,
			BaseContext: func(_ net.Listener) context.Context {
				return a.Context
			},
		}*/
	}
	errorGroup.Go(func() error {
		a.RuntimeIsActive = true
		log.Log.Infof("Listening at %s... start", a.Address)
		//err := a.RuntimeServer.ListenAndServe()
		err := a.HTTPServer.Listen(a.Address)
		a.RuntimeIsActive = false
		log.Log.Infof("Listening at %s... stopped (%v)", a.Address, err)
		return err
	})

	return nil
}

func (a *DXAPI) StartShutdown() (err error) {
	if a.RuntimeIsActive {
		log.Log.Infof("Shutdown api %s start...", a.NameId)
		err = a.HTTPServer.ShutdownWithContext(core.RootContext)
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
