package api

import (
	"context"
	"dxlib/v3/log"
	utilsHttp "dxlib/v3/utils/http"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"net/http"
	"sort"
)

type DXAPIEndPointType int

const (
	EndPointTypeHTTP DXAPIEndPointType = iota
	EndPointTypeWS
)

type DXAPIEndPointParameter struct {
	Owner       *DXAPIEndPoint
	Parent      *DXAPIEndPointParameter
	NameId      string
	Type        string
	Description string
	IsMustExist bool
	Children    []DXAPIEndPointParameter
}

func (aep *DXAPIEndPointParameter) PrintSpec(leftIndent int64) (s string) {
	switch SpecFormat {
	case "MarkDown":
		r := ""
		if aep.IsMustExist {
			r = "mandatory"
		} else {
			r = "optional"
		}
		s += fmt.Sprintf("%*s - %s (%s) %s %s\n", leftIndent, "", aep.NameId, aep.Type, r, aep.Description)
		if len(aep.Children) > 0 {
			for _, c := range aep.Children {
				s += c.PrintSpec(leftIndent + 2)
			}
		}
	}

	return s
}

type DxAPIEndPointResponsePossibility struct {
	Owner        *DXAPIEndPoint
	StatusCode   int
	Description  string
	Headers      map[string]string
	DataTemplate []*DXAPIEndPointParameter
}

type DXAPIEndPointExecuteFunc func(aepr *DXAPIEndPointRequest) (err error)

type DXAPIEndPoint struct {
	Owner                 *DXAPI
	Title                 string
	Uri                   string
	Method                string
	EndPointType          DXAPIEndPointType
	Description           string
	RequestContentType    utilsHttp.RequestContentType
	Parameters            []DXAPIEndPointParameter
	OnExecute             DXAPIEndPointExecuteFunc
	OnWSLoop              DXAPIEndPointExecuteFunc
	ResponsePossibilities map[string]*DxAPIEndPointResponsePossibility
}

func (aep *DXAPIEndPoint) PrintSpec() (s string) {
	switch SpecFormat {
	case "MarkDown":
		s = fmt.Sprintf("## %s\n", aep.Title)
		s += fmt.Sprintf("####  Description: %s\n", aep.Description)
		s += fmt.Sprintf("####  URI: %s\n", aep.Uri)
		s += fmt.Sprintf("####  Method: %s\n", aep.Method)
		s += fmt.Sprintf("####  Request Content Type: %s\n", aep.RequestContentType)
		s += "####  Parameters:\n"
		for _, p := range aep.Parameters {
			s += p.PrintSpec(4)
		}
		s += "####  Response Possibilities:\n"
		keys := make([]string, 0, len(aep.ResponsePossibilities))

		// Add the keys to the slice
		for k := range aep.ResponsePossibilities {
			keys = append(keys, k)
		}

		// Sort the keys based on StatusCode
		sort.Slice(keys, func(i, j int) bool {
			return aep.ResponsePossibilities[keys[i]].StatusCode < aep.ResponsePossibilities[keys[j]].StatusCode
		})

		// Now you can range over the keys slice and use it to access the map
		for _, k := range keys {
			v := aep.ResponsePossibilities[k]
			fmt.Println("Key:", k, "StatusCode:", aep.ResponsePossibilities[k].StatusCode)
			s += fmt.Sprintf("    %s\n", k)
			s += fmt.Sprintf("      Status Code: %d\n", v.StatusCode)
			s += fmt.Sprintf("      Description: %s\n", v.Description)
			s += "      Headers:\n"
			for hk, hv := range v.Headers {
				s += fmt.Sprintf("        %s: %s\n", hk, hv)
			}
			s += "      Data Template:\n"
			for _, p := range v.DataTemplate {
				s += p.PrintSpec(8)
			}
		}
	}

	return s
}

func (aep *DXAPIEndPoint) NewParameter(parent *DXAPIEndPointParameter, nameId, aType, description string, isMustExist bool) *DXAPIEndPointParameter {
	p := DXAPIEndPointParameter{Owner: aep, NameId: nameId, Type: aType, Description: description, IsMustExist: isMustExist}
	p.Parent = parent
	aep.Parameters = append(aep.Parameters, p)
	return &p
}

func (aep *DXAPIEndPoint) NewEndPointRequest(context context.Context, c *fiber.Ctx) *DXAPIEndPointRequest {
	er := &DXAPIEndPointRequest{
		Context:         context,
		FiberContext:    c,
		EndPoint:        aep,
		ParameterValues: map[string]*DXAPIEndPointRequestParameterValue{},
		//ResponseWriter:        w,
		//Request:               r,
		ResponseStatusCode:    http.StatusOK,
		ResponseErrorAsString: "",
		ResponseBodyAsBytes:   nil,
	}
	er.Id = fmt.Sprintf("%p", er)
	er.Log = log.NewLog(&aep.Owner.Log, context, aep.Title+" | "+er.Id)
	return er
}
