package testing

import (
	dxlibv3HttpClient "dxlib/v3/utils/http/client"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httputil"
	"testing"

	"dxlib/v3/utils"
	json2 "dxlib/v3/utils/json"
)

var Counter = 0

func DoHTTPClientTest(t *testing.T, mustSuccess bool, testName, method, url, contentType string, body []byte) *http.Response {
	defer func() {
		t.Logf("== Testing %s\n DONE ==", testName)
	}()

	t.Logf("== Testing %s\n START ==", testName)

	request, response, err := dxlibv3HttpClient.HTTPClient(method, url, dxlibv3HttpClient.HTTPHeader{
		"Content-Type":  contentType,
		"Cache-Control": "no-cache",
	}, body)

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		t.Logf(`Error in DumpRequest (%v)`, err)
		return nil
	}
	t.Logf("\nRaw Request :\n%v\n", string(requestDump))

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		t.Logf(`Error in DumpResponse (%v)`, err)
		t.FailNow()
		return response
	}
	t.Logf("\nRaw Response: \n%v\n", string(responseDump))

	if mustSuccess {
		if response.StatusCode != 200 {
			t.Logf(`Error: should be success but not (%v)`, response.StatusCode)
			t.FailNow()
			return response
		}
		return response
	}
	if response.StatusCode != 200 {
		return response
	}
	t.Logf(`Error: should be fail but not (%v)`, response.StatusCode)
	t.FailNow()
	return response
}

func ResponseBodyToJSON(t *testing.T, r *http.Response) (utils.JSON, error) {
	if r.StatusCode != 200 {
		err := fmt.Errorf("response status code is not 200 (%v)", r.StatusCode)
		t.Log(err.Error())
		t.FailNow()
		return nil, err
	}
	v := utils.JSON{}
	bodyAll, err := io.ReadAll(r.Body)
	if err != nil {
		t.Logf("Error in reading all response body %v", r)
		t.FailNow()
		return nil, err
	}

	err = json.Unmarshal(bodyAll, &v)
	if err != nil {
		t.Logf("Error in unmarshall the response %v", r)
		t.FailNow()
		return nil, err
	}

	vAsString, err := json2.PrettyPrint(v)
	if err != nil {
		t.Logf("Error in pretty print the response %v", r)
		t.FailNow()
		return nil, err
	}

	t.Logf("response=\n%s\n", vAsString)
	return v, nil
}

/*
* Style0: No 'code' on response body JSON
* Style1: 'code' on response body JSON
 */

func Style0HTTPClientTest(t *testing.T, mustSuccess bool, testName, method, url, contentType string, body []byte) (r utils.JSON) {
	r1 := DoHTTPClientTest(t, mustSuccess, testName, method, url, contentType, body)
	if r1.ContentLength > 0 {
		v, err := ResponseBodyToJSON(t, r1)
		if err != nil {
			t.FailNow()
			return
		}

		vAsString, err := json2.PrettyPrint(v)
		if err != nil {
			t.Logf("Error in marshall the data %v", r1)
			t.FailNow()
			return v
		}
		t.Logf("\nResponse(JSON):\n%s\n", vAsString)
		if mustSuccess {
			if r1.StatusCode != 200 {
				t.Logf("StatusCode should be 200 OK, but has value=%v", r1.StatusCode)
				t.FailNow()
				return v
			}
		} else {
			t.Logf("Code should be not 200 OK, has value=%v", r1.StatusCode)
			return v
		}
		return v
	}
	return nil
}

func Style1HTTPClientTest(t *testing.T, mustSuccess bool, testName, method, url, contentType string, body []byte) (r utils.JSON) {
	r1 := DoHTTPClientTest(t, mustSuccess, testName, method, url, contentType, body)
	if r1.ContentLength > 0 {
		v, err := ResponseBodyToJSON(t, r1)
		if err != nil {
			t.FailNow()
			return
		}

		vAsString, err := json2.PrettyPrint(v)
		if err != nil {
			t.Logf("Error in marshall the data %v", r1)
			t.FailNow()
			return v
		}
		t.Logf("\nResponse=\n%s\n", vAsString)
		code, ok := v["code"].(string)
		if !ok {
			t.Logf("Error in get the field 'code' %v", r1)
			t.FailNow()
			return v
		}
		if mustSuccess {
			if code != "OK" {
				t.Logf("Code should be OK, but has value=%v", code)
				t.FailNow()
				return v
			}
		} else {
			t.Logf("Code should be not OK, has value=%v", code)
			return v
		}
		return v
	}
	return nil
}

func THTTPClient(t *testing.T, mustStatusCode int, method string, url string, contentType string, body string) (responseBodyAsString string) {
	Counter++
	v := Counter
	t.Logf("%d: ==== TEST START ====\nREQUEST ===\n%s %s\nContentType: %s\nBody:\n%s\n==\n\n", v, method, url, contentType, body)
	_, response, err := dxlibv3HttpClient.HTTPClientReadAll(method, url, map[string]string{"Content-Type": contentType}, body)
	if err != nil {
		t.Logf("EXECUTE ERROR === Error in making HTTP request %v\n", err)
		t.FailNow()
	}
	statusCode := response.StatusCode
	responseBodyAsString = response.BodyAsString()

	t.Logf("RESPONSE ===\n%d\nBody:\n%s\n===\n\n", statusCode, responseBodyAsString)
	assert.Equal(t, mustStatusCode, statusCode)
	t.Logf("%d: ==== TEST END   ====\n", v)
	return responseBodyAsString
}
