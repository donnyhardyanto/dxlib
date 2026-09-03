package api

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
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
	// EndPointTypeHTTPEndToEndEncryptionV3 — persistent-session inner envelope.
	// Bootstrap at /v1/startup_1 establishes a global connection_id and a
	// per-session AES key in Redis. Bulk requests carry { connection_id, data }.
	// See OnE2EEV3Unpack / OnE2EEV3Pack hooks in api/e2ee_v3.go.
	EndPointTypeHTTPEndToEndEncryptionV3
	// EndPointTypeHTTPEndToEndEncryptionV4 — same as V3 but uses LVLE
	// (4-byte little-endian length prefix) instead of LV (big-endian).
	// See OnE2EEV4Unpack / OnE2EEV4Pack hooks in api/e2ee_v4.go.
	EndPointTypeHTTPEndToEndEncryptionV4
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
	case EndPointTypeHTTPEndToEndEncryptionV3:
		return "EndPointTypeHTTPEndToEndEncryptionV3"
	case EndPointTypeHTTPEndToEndEncryptionV4:
		return "EndPointTypeHTTPEndToEndEncryptionV4"
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
	Enum        []any
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
		if len(aep.Enum) > 0 {
			var enumBuilder strings.Builder
			enumBuilder.WriteString("[")
			for i, v := range aep.Enum {
				if i > 0 {
					enumBuilder.WriteString(", ")
				}
				// Always wrap enum values in quotes for consistent spec output
				enumBuilder.WriteString(fmt.Sprintf(`"%v"`, v))
			}
			enumBuilder.WriteString("]")
			s += fmt.Sprintf("%*s   Possible values: %s\n", leftIndent, "", enumBuilder.String())
		}
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

// DXAPIEndPointWSOpenFunc runs once per connection, before the read loop starts.
// Returning an error closes the connection without running OnWSClose.
type DXAPIEndPointWSOpenFunc func(aepr *DXAPIEndPointRequest) (err error)

// DXAPIEndPointWSMessageFunc handles one inbound frame. A non-empty response is
// written straight back to the sender. This is where an application's own
// protocol lives -- the library never looks inside the bytes.
type DXAPIEndPointWSMessageFunc func(aepr *DXAPIEndPointRequest, message []byte) (response []byte, err error)

// DXAPIEndPointWSCloseFunc runs once, after the read loop ends, however it ended.
type DXAPIEndPointWSCloseFunc func(aepr *DXAPIEndPointRequest)

// DXAPIEndPointWSPeriodicFunc runs on every WSPeriodicInterval tick for as long
// as the connection is open, for whatever an application needs to push without
// being asked.
type DXAPIEndPointWSPeriodicFunc func(aepr *DXAPIEndPointRequest) (err error)

type DXAPIEndPointResponsePossibilities map[string]DXAPIEndPointResponsePossibility
type DXAPIEndPoint struct {
	Owner              *DXAPI
	Title              string
	Uri                string
	Method             string
	EndPointType       DXAPIEndPointType
	Description        string
	RequestContentType utilsHttp.RequestContentType
	Parameters         []DXAPIEndPointParameter
	OnExecute          DXAPIEndPointExecuteFunc
	OnWSLoop           DXAPIEndPointExecuteFunc

	// The hooks below drive the built-in WebSocket loop, used when OnWSLoop is
	// nil. Each application differs only in what it does on open and close and
	// in how it reads and writes a message, so those are all that is left to
	// supply; the read loop, write pump, keepalive and client registry are the
	// same everywhere and live in the library.
	OnWSOpen           DXAPIEndPointWSOpenFunc
	OnWSMessage        DXAPIEndPointWSMessageFunc
	OnWSClose          DXAPIEndPointWSCloseFunc
	OnWSPeriodic       DXAPIEndPointWSPeriodicFunc
	WSPeriodicInterval time.Duration

	// Endpoints are copied by value in places, so the client set sits behind a
	// pointer: every copy then shares the one registry instead of carrying a
	// lock that cannot legally be copied.
	wsClients *wsClientSet

	ResponsePossibilities   *DXAPIEndPointResponsePossibilities
	Middlewares             []DXAPIEndPointExecuteFunc
	Privileges              []string
	RequestMaxContentLength int64
	RateLimitGroupNameId    string
}

func (aep *DXAPIEndPoint) PrintSpec() (s string, err error) {
	// ResponsePossibilities is dereferenced five times below -- four in the
	// MarkDown branch and once in PostmanCollection -- and NewEndPoint has no
	// in-library callers, so the pointer comes entirely from downstream apps with
	// nothing validating it. A nil made GET /spec panic for the whole document,
	// because APIHandlerPrintSpec serves this over HTTP. An empty set makes every
	// use below a no-op.
	if aep.ResponsePossibilities == nil {
		aep.ResponsePossibilities = &DXAPIEndPointResponsePossibilities{}
	}

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
		keys := make([]string, 0, len(*aep.ResponsePossibilities))

		// Add the keys to the slice
		for k := range *aep.ResponsePossibilities {
			keys = append(keys, k)
		}

		// Sort the keys based on StatusCode
		sort.Slice(keys, func(i, j int) bool {
			return (*aep.ResponsePossibilities)[keys[i]].StatusCode < (*aep.ResponsePossibilities)[keys[j]].StatusCode
		})

		// Now you can range over the keys slice and use it to access the map
		for _, k := range keys {
			v := (*aep.ResponsePossibilities)[k]
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

		for _, resp := range *aep.ResponsePossibilities {
			collection["item"].([]map[string]any)[0]["response"] = append(collection["item"].([]map[string]any)[0]["response"].([]map[string]any), map[string]any{
				"name":   resp.Description,
				"status": http.StatusText(resp.StatusCode),
				"code":   resp.StatusCode,
				"body":   "",
			})
		}

		collectionJSON, err := json.MarshalIndent(collection, "", "  ")
		if err != nil {
			return "", errors.Wrap(err, "API_ENDPOINT_SPEC_JSON_MARSHAL_ERROR")
		}

		return string(collectionJSON), nil
	default:
		return "", errors.Errorf("SpecFormat %s is not supported", SpecFormat)
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
	er.Log.RequestURL = r.URL.Path
	er.PeerCertificate = VerifiedPeerCertificate(r)
	er.PeerIdentity = utilsTLS.PeerIdentity(er.PeerCertificate)
	return er
}

// VerifiedPeerCertificate is the client certificate the TLS layer verified for
// this request, or nil. It reads VerifiedChains, not PeerCertificates: under
// the "request" migration rung Go fills PeerCertificates with whatever the
// client sent and verifies none of it, and treating that as an identity would let a
// caller choose its own. When VerifiedChains is non-empty its first chain's
// first element is the leaf, and it is the same object as PeerCertificates[0].
func VerifiedPeerCertificate(r *http.Request) *x509.Certificate {
	if r == nil || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return nil
	}
	return r.TLS.VerifiedChains[0][0]
}

// PeerIdentityFromRequest is utilsTLS.PeerIdentity of VerifiedPeerCertificate,
// for the audit entry written before the request object exists.
func PeerIdentityFromRequest(r *http.Request) string {
	return utilsTLS.PeerIdentity(VerifiedPeerCertificate(r))
}
