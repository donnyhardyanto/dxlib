package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

type DXAPIEndPointType int

const (
	EndPointTypeHTTPJSON DXAPIEndPointType = iota
	EndPointTypeHTTPUploadStream
	EndPointTypeHTTPDownloadStream
	EndPointTypeHTTPDownloadStreamV2
	EndPointTypeWS
	EndPointTypeHTTPEndToEndEncryptionV1
	EndPointTypeHTTPEndToEndEncryptionV2
)

func (d DXAPIEndPointType) String() string {
	switch d {
	case EndPointTypeHTTPJSON:
		return "EndPointTypeHTTPJSON"
	case EndPointTypeHTTPUploadStream:
		return "EndPointTypeHTTPUploadStream"
	case EndPointTypeHTTPDownloadStream:
		return "EndPointTypeHTTPDownloadStream"
	case EndPointTypeHTTPDownloadStreamV2:
		return "EndPointTypeHTTPDownloadStreamV2"
	case EndPointTypeWS:
		return "EndPointTypeWS"
	case EndPointTypeHTTPEndToEndEncryptionV1:
		return "EndPointTypeHTTPEndToEndEncryptionV1"
	case EndPointTypeHTTPEndToEndEncryptionV2:
		return "EndPointTypeHTTPEndToEndEncryptionV2"
	default:
		return fmt.Sprintf("DXAPIEndPointType(%d)", d)
	}
}

type DXAPIEndPointParameter struct {
	Owner       *DXAPIEndPoint
	Parent      *DXAPIEndPointParameter
	NameId      string
	Type        dxlibTypes.APIParameterType
	Description string
	IsMustExist bool
	IsNullable  bool
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
	case "PostmanCollection":
		return fmt.Sprintf("    - name: %s\n    - description: %s\n    - type: %s\n    - required: %t\n    - nullable: %t\n", aep.NameId, aep.Description, aep.Type, aep.IsMustExist, aep.IsNullable)
	default:
		return ""

	}

	return s
}

type DXAPIEndPointResponsePossibility struct {
	Owner        *DXAPIEndPoint
	StatusCode   int
	Description  string
	Headers      map[string]string
	DataTemplate []*DXAPIEndPointParameter
}

type DXAPIEndPointExecuteFunc func(aepr *DXAPIEndPointRequest) (err error)

type DXAPIEndPoint struct {
	Owner                   *DXAPI
	Title                   string
	Uri                     string
	Method                  string
	EndPointType            DXAPIEndPointType
	Description             string
	RequestContentType      utilsHttp.RequestContentType
	Parameters              []DXAPIEndPointParameter
	OnExecute               DXAPIEndPointExecuteFunc
	OnWSLoop                DXAPIEndPointExecuteFunc
	ResponsePossibilities   map[string]*DXAPIEndPointResponsePossibility
	Middlewares             []DXAPIEndPointExecuteFunc
	Privileges              []string
	RequestMaxContentLength int64
	RateLimitGroupNameId    string
}

func (aep *DXAPIEndPoint) PrintSpec() (s string, err error) {
	switch SpecFormat {
	case "MarkDown":
		s = fmt.Sprintf("## %s\n", aep.Title)
		s += fmt.Sprintf("####  Description: %s\n", aep.Description)
		s += fmt.Sprintf("####  URI: %s\n", aep.Uri)
		s += fmt.Sprintf("####  Method: %s\n", aep.Method)
		s += fmt.Sprintf("####  Endpoint Type:%s\n", aep.EndPointType)
		s += fmt.Sprintf("####  Request Content Type: %s\n", aep.RequestContentType)
		s += fmt.Sprintf("####  Request Content Length: %d\n", aep.RequestMaxContentLength)
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
	case "PostmanCollection":
		collection := map[string]any{
			"info": map[string]any{
				"name":        aep.Title,
				"description": aep.Description,
				"schema":      "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
			},
			"item": []map[string]any{
				{
					"name": aep.Title,
					"request": map[string]any{
						"method":      aep.Method,
						"description": aep.Description,
						"url": map[string]any{
							"raw":      aep.Uri,
							"protocol": "http",
							"host":     []string{"{{base_url}}"},
							"path":     []string{aep.Uri},
						},
						"body": map[string]any{
							"mode": "raw",
							"raw":  "",
						},
					},
					"response": []map[string]any{},
				},
			},
		}

		for _, param := range aep.Parameters {
			rawBody := collection["item"].([]map[string]any)[0]["request"].(map[string]any)["body"].(map[string]any)["raw"].(string)
			rawBody += fmt.Sprintf("%s: %s\n", param.NameId, param.Type)
			collection["item"].([]map[string]any)[0]["request"].(map[string]any)["body"].(map[string]any)["raw"] = rawBody
		}

		for _, resp := range aep.ResponsePossibilities {
			collection["item"].([]map[string]any)[0]["response"] = append(collection["item"].([]map[string]any)[0]["response"].([]map[string]any), map[string]any{
				"name":   resp.Description,
				"status": http.StatusText(resp.StatusCode),
				"code":   resp.StatusCode,
				"body":   "",
			})
		}

		collectionJSON, err := json.MarshalIndent(collection, "", "  ")
		if err != nil {
			return "", err
		}

		return string(collectionJSON), nil
	default:
		return "", fmt.Errorf("SpecFormat %s is not supported", SpecFormat)
	}

	return s, nil
}

func (aep *DXAPIEndPoint) NewParameter(parent *DXAPIEndPointParameter, nameId string, aType dxlibTypes.APIParameterType, description string, isMustExist bool) *DXAPIEndPointParameter {
	nameId = strings.TrimSpace(nameId)
	description = strings.TrimSpace(description)
	p := DXAPIEndPointParameter{Owner: aep, NameId: nameId, Type: aType, Description: description, IsMustExist: isMustExist}
	switch aType {
	case dxlibTypes.APIParameterTypeNullableInt64:
		p.IsNullable = true
	case "nullable-string":
		p.IsNullable = true
	default:
		p.IsNullable = false
	}
	p.Parent = parent
	aep.Parameters = append(aep.Parameters, p)
	return &p
}

func (aep *DXAPIEndPoint) NewEndPointRequest(context context.Context, w http.ResponseWriter, r *http.Request) *DXAPIEndPointRequest {
	er := &DXAPIEndPointRequest{
		Context:         context,
		ResponseWriter:  &w,
		Request:         r,
		EndPoint:        aep,
		ParameterValues: map[string]*DXAPIEndPointRequestParameterValue{},
		LocalData:       map[string]any{},
		SuppressLogDump: false,
	}
	er.Id = fmt.Sprintf("%p", er)
	er.Log = log.NewLog(&aep.Owner.Log, context, aep.Title+" | "+er.Id)
	return er
}
