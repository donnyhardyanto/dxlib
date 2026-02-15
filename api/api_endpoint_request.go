package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/language"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	"github.com/donnyhardyanto/dxlib/utils/lv"
	"go.opentelemetry.io/otel/trace"
)

type DXAPIUser struct {
	Id               string
	Uid              string
	LoginId          string
	FullName         string
	OrganizationId   string
	OrganizationUid  string
	OrganizationName string
}

type DXAPIEndPointRequest struct {
	Id                     string
	Context                context.Context
	EndPoint               *DXAPIEndPoint
	ParameterValues        map[string]*DXAPIEndPointRequestParameterValue
	Log                    log.DXLog
	Request                *http.Request
	RequestBodyAsBytes     []byte
	ResponseWriter         *http.ResponseWriter
	_responseErrorAsString string
	ResponseStatusCode     int
	ErrorMessage           []string
	CurrentUser            DXAPIUser
	LocalData              utils.JSON
	ResponseHeaderSent     bool
	ResponseBodySent       bool
	SuppressLogDump        bool
	EncryptionParameters   utils.JSON
	EffectiveRequestHeader map[string]string
}

func (aepr *DXAPIEndPointRequest) GetParameterValues() (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range aepr.ParameterValues {
		r[k] = v.Value
	}
	return r
}

// TranslateMessage translates a message key using the user's language from session.
// Falls back to system default language ('id') if user language is not set.
// Returns the original key if translation is not found.
func (aepr *DXAPIEndPointRequest) TranslateMessage(messageKey string) string {
	// Get language from session (populated by SessionKeyToSessionObject)
	userLanguageStr, ok := aepr.LocalData["language"].(string)
	if !ok || userLanguageStr == "" {
		userLanguageStr = "id" // Default to Indonesian
	}

	// Convert string to DXLanguage type
	userLanguage := language.DXLanguage(userLanguageStr)

	// Translate using the language package with original fallback mode
	translated := language.Translate(messageKey, userLanguage, language.DXTranslateFallbackModeOriginal)
	return translated
}

// TranslateMessageWithArgs translates a message key and formats it with arguments.
// Uses fmt.Sprintf for formatting after translation.
// Falls back to system default language ('id') if user language is not set.
// Returns formatted original key if translation is not found.
func (aepr *DXAPIEndPointRequest) TranslateMessageWithArgs(messageKey string, args ...any) string {
	// Get language from session (populated by SessionKeyToSessionObject)
	userLanguageStr, ok := aepr.LocalData["language"].(string)
	if !ok || userLanguageStr == "" {
		userLanguageStr = "id" // Default to Indonesian
	}

	// Convert string to DXLanguage type
	userLanguage := language.DXLanguage(userLanguageStr)

	// Translate the template with original fallback mode
	template := language.Translate(messageKey, userLanguage, language.DXTranslateFallbackModeOriginal)

	// Format with arguments
	if len(args) > 0 {
		return fmt.Sprintf(template, args...)
	}
	return template
}

// logResponseTrace logs response phase trace information for Grafana monitoring
func (aepr *DXAPIEndPointRequest) logResponseTrace(phase string, startTime time.Time, statusCode int, errMsg string) {
	spanCtx := trace.SpanFromContext(aepr.Context).SpanContext()
	traceId := spanCtx.TraceID().String()
	spanId := spanCtx.SpanID().String()

	durationMs := float64(time.Since(startTime).Microseconds()) / 1000.0

	endpoint := ""
	method := ""
	if aepr.EndPoint != nil {
		endpoint = aepr.EndPoint.Uri
	}
	if aepr.Request != nil {
		method = aepr.Request.Method
	}

	attrs := []any{
		slog.String("trace_id", traceId),
		slog.String("span_id", spanId),
		slog.String("request_id", aepr.Id),
		slog.String("phase", phase),
		slog.String("endpoint", endpoint),
		slog.String("method", method),
		slog.Float64("duration_ms", durationMs),
		slog.Int("status_code", statusCode),
		slog.Int("body_size", len(aepr.RequestBodyAsBytes)),
	}

	if errMsg != "" {
		attrs = append(attrs, slog.String("error", errMsg))
	}

	slog.Info("EXECUTION_TRACE", attrs...)
}

func (aepr *DXAPIEndPointRequest) RequestDump() ([]byte, error) {
	var b bytes.Buffer

	// By default, print out the unmodified req.RequestURI, which
	// is always set for incoming server requests. But because we
	// previously used req.URL.RequestURI and the docs weren't
	// always so clear about when to use DumpRequest vs.
	// DumpRequestOut, fall back to the old way if the caller
	// provides a non-server Request.
	req := aepr.Request
	reqURI := req.RequestURI
	if reqURI == "" {
		reqURI = req.URL.RequestURI()
	}

	_, _ = fmt.Fprintf(&b, "%s %s HTTP/%d.%d\r\n", req.Method, reqURI, req.ProtoMajor, req.ProtoMinor)

	absRequestURI := strings.HasPrefix(reqURI, "http://") || strings.HasPrefix(reqURI, "https://")
	if !absRequestURI {
		host := req.Host
		if host == "" && req.URL != nil {
			host = req.URL.Host
		}
		if host != "" {
			_, _ = fmt.Fprintf(&b, "Host: %s\r\n", host)
		}
	}

	if len(req.TransferEncoding) > 0 {
		_, _ = fmt.Fprintf(&b, "Transfer-Encoding: %s\r\n", strings.Join(req.TransferEncoding, ","))
	}

	var reqWriteExcludeHeaderDump = map[string]bool{
		"Host":                true, // not in Header map anyway
		"Transfer-Encoding":   true,
		"Trailer":             true,
		"Authorization":       true,
		"Proxy-Authorization": true,
		"Cookie":              true,
		"Set-Cookie":          true,
		"X-Api-Key":           true,
		"X-Auth-Token":        true,
	}

	err := req.Header.WriteSubset(&b, reqWriteExcludeHeaderDump)
	if err != nil {
		return nil, err
	}

	_, _ = io.WriteString(&b, "\r\n")
	b.Write(aepr.RequestBodyAsBytes)
	_, err = io.WriteString(&b, "\r\n\r\n")

	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (aepr *DXAPIEndPointRequest) RequestDumpAsString() (string, error) {
	b, err := aepr.RequestDump()
	return string(b), err
}

// DecryptedRequestDumpAsString returns a formatted dump of decrypted request headers and body parameters
// with sensitive fields masked. This is useful for debugging E2E encrypted requests.
func (aepr *DXAPIEndPointRequest) DecryptedRequestDumpAsString() string {
	var b strings.Builder

	// Dump effective (decrypted) headers
	b.WriteString("Decrypted Headers:\n")
	if aepr.EffectiveRequestHeader != nil {
		for k, v := range aepr.EffectiveRequestHeader {
			maskedValue := utils.MaskSensitiveValue(k, v)
			b.WriteString(fmt.Sprintf("%s: %v\n", k, maskedValue))
		}
	} else {
		b.WriteString("(no decrypted headers)\n")
	}

	// Dump decrypted body parameters
	b.WriteString("\nDecrypted Body Parameters:\n")
	params := aepr.GetParameterValues()
	if len(params) > 0 {
		// Mask sensitive fields before marshaling
		maskedParams := utils.JSON{}
		for k, v := range params {
			maskedParams[k] = utils.MaskSensitiveValue(k, v)
		}

		paramsJSON, err := json.MarshalIndent(maskedParams, "", "  ")
		if err != nil {
			b.WriteString(fmt.Sprintf("ERROR_MARSHALING_PARAMS: %v\n", err))
		} else {
			b.Write(paramsJSON)
			b.WriteString("\n")
		}
	} else {
		b.WriteString("(no parameters)\n")
	}

	return b.String()
}

func (aepr *DXAPIEndPointRequest) GetResponseWriter() *http.ResponseWriter {
	return aepr.ResponseWriter
}

func (aepr *DXAPIEndPointRequest) WriteResponseAndNewErrorf(statusCode int, responseMessage string, msg string, data ...any) (err error) {
	if responseMessage == "" {
		responseMessage = strings.ToUpper(http.StatusText(statusCode))
	}
	if msg == "" {
		msg = responseMessage
	} else {
		if data != nil {
			msg = fmt.Sprintf(msg, data...)
		}
	}
	err = aepr.Log.ErrorAndCreateErrorf(msg)
	aepr.WriteResponseAsErrorMessageNotLogged(statusCode, responseMessage, msg)
	return err
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsString(statusCode int, header map[string]string, s string) {
	aepr.WriteResponseAsBytes(statusCode, header, []byte(s))
}

func (aepr *DXAPIEndPointRequest) WriteResponseAndLogAsError(statusCode int, responseMessage string, err error) {
	if responseMessage == "" {
		responseMessage = strings.ToUpper(http.StatusText(statusCode))
	}
	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = "DUMP REQUEST FAIL"
	}

	// Add decrypted request dump if parameters are available
	decryptedDump := ""
	if aepr.EffectiveRequestHeader != nil || len(aepr.ParameterValues) > 0 {
		decryptedDump = "\n\n" + aepr.DecryptedRequestDumpAsString()
	}

	fullDump := requestDump + decryptedDump
	aepr.Log.LogText(err, log.DXLogLevelError, "", fullDump)
	aepr.WriteResponseAsErrorMessageNotLogged(statusCode, responseMessage, err.Error())
	return
}

func (aepr *DXAPIEndPointRequest) WriteResponseAndLogAsErrorf(statusCode int, responseMessage string, msg string, data ...any) (err error) {
	if msg == "" {
		msg = responseMessage
	} else {
		if data != nil {
			msg = fmt.Sprintf(msg, data...)
		}
	}

	if responseMessage == "" {
		responseMessage = strings.ToUpper(http.StatusText(statusCode))
	}

	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = "DUMP REQUEST FAIL"
	}

	// Add decrypted request dump if parameters are available
	decryptedDump := ""
	if aepr.EffectiveRequestHeader != nil || len(aepr.ParameterValues) > 0 {
		decryptedDump = "\n\n" + aepr.DecryptedRequestDumpAsString()
	}

	err = errors.New(msg)
	fullDump := requestDump + decryptedDump
	aepr.Log.LogText(err, log.DXLogLevelError, "", fullDump)
	aepr.WriteResponseAsErrorMessageNotLogged(statusCode, responseMessage, msg)

	return err
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsError(statusCode int, errToSend error) {
	if aepr.ResponseHeaderSent {
		return
	}
	if (200 <= statusCode) && (statusCode < 300) {
		statusCode = 500
	}
	var s utils.JSON

	//	if dxlib.IsDebug {
	s = utils.JSON{
		"status":         http.StatusText(statusCode),
		"status_code":    statusCode,
		"reason":         errToSend.Error(),
		"reason_message": errToSend.Error(),
	}
	//	}

	aepr.WriteResponseAsJSON(statusCode, nil, s)
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsErrorMessageNotLogged(statusCode int, errorMsg string, reasonMsg string) {
	if aepr.ResponseHeaderSent {
		return
	}
	if (200 <= statusCode) && (statusCode < 300) {
		statusCode = 500
	}
	var s utils.JSON

	//	if dxlib.IsDebug {
	s = utils.JSON{
		"status":         http.StatusText(statusCode),
		"status_code":    statusCode,
		"reason":         errorMsg,
		"reason_message": reasonMsg,
	}
	//	}

	aepr.WriteResponseAsJSON(statusCode, nil, s)
	return
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsJSON(statusCode int, header map[string]string, bodyAsJSON utils.JSON) {
	if aepr.ResponseHeaderSent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_HEADER_ALREADY_SENT")
		return
	}
	var jsonBytes []byte
	var err error
	if bodyAsJSON == nil {
		bodyAsJSON = utils.JSON{}
	}
	if bodyAsJSON["status"] == nil {
		bodyAsJSON["status"] = http.StatusText(statusCode)
	}
	if bodyAsJSON["status_code"] == nil {
		bodyAsJSON["status_code"] = statusCode
	}
	if bodyAsJSON["reason"] == nil {
		if statusCode == 200 {
			bodyAsJSON["reason"] = "OK"
		} else {
			bodyAsJSON["reason"] = http.StatusText(statusCode)
		}
	}
	if bodyAsJSON["reason_message"] == nil {
		if statusCode == 200 {
			bodyAsJSON["reason_message"] = "OK"
		} else {
			bodyAsJSON["reason_message"] = http.StatusText(statusCode)
		}
	}

	// Extract ERROR_LOG= from reason_message and move to error_log_ref
	if reasonMessage, ok := bodyAsJSON["reason_message"].(string); ok {
		if strings.Contains(reasonMessage, "ERROR_LOG=") {
			// Extract error log reference
			parts := strings.Split(reasonMessage, "ERROR_LOG=")
			if len(parts) >= 2 {
				// Store error log reference in separate field
				bodyAsJSON["error_log_ref"] = strings.TrimSpace(parts[1])
				// Remove ERROR_LOG= from reason_message
				bodyAsJSON["reason_message"] = strings.TrimSpace(parts[0])
			}
		}
	}

	// Translate status, reason, and reason_message using user's language
	if status, ok := bodyAsJSON["status"].(string); ok {
		bodyAsJSON["status"] = aepr.TranslateMessage(status)
	}
	if reason, ok := bodyAsJSON["reason"].(string); ok {
		bodyAsJSON["reason"] = aepr.TranslateMessage(reason)
	}
	if reasonMessage, ok := bodyAsJSON["reason_message"].(string); ok {
		bodyAsJSON["reason_message"] = aepr.TranslateMessage(reasonMessage)
	}

	jsonBytes, err = json.Marshal(bodyAsJSON)
	if err != nil {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_MARSHAL_JSON=%s", err.Error())
		return
	}

	// Log response before encryption for debugging
	if statusCode != http.StatusOK {
		aepr.Log.Infof("RESPONSE_DUMP_BEFORE_ENCRYPT:\n%s", string(jsonBytes))
	}

	if header == nil {
		header = map[string]string{}
	}
	header["Content-Type"] = "application/json"

	aepr.WriteResponseAsBytes(statusCode, header, jsonBytes)
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsBytes(statusCode int, header map[string]string, bodyAsBytes []byte) {
	// TRACE: response_write_start
	responseWriteStartTime := time.Now()
	aepr.logResponseTrace("response_write_start", responseWriteStartTime, statusCode, "")

	if aepr.ResponseHeaderSent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_HEADER_ALREADY_SENT")
		aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "RESPONSE_HEADER_ALREADY_SENT")
		return
	}
	responseWriter := *aepr.GetResponseWriter()

	switch aepr.EndPoint.EndPointType {
	case EndPointTypeHTTPEndToEndEncryptionV2:
		// Check if EncryptionParameters was populated during request preprocessing
		// If nil, it means preprocessing failed (used/expired prekey, replay attack, etc.)
		// Send unencrypted error telling client to refresh prekey
		if aepr.EncryptionParameters == nil {
			aepr.Log.Warnf("E2EE_ENCRYPTION_PARAMETERS_NIL:PREKEY_MISSING_OR_USED:status=%d", statusCode)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "E2EE_PREKEY_MISSING_FALLBACK_TO_PLAIN")

			// Determine if this is a captcha endpoint
			isCaptchaEndpoint := strings.Contains(aepr.EndPoint.Uri, "captcha")

			// Send plain error response instructing client to refresh prekey/captcha
			// This happens when:
			// 1. Prekey already used (deleted after first use)
			// 2. Prekey expired (TTL elapsed)
			// 3. Replay attack (same prekey reused)
			var errorResponse utils.JSON
			if isCaptchaEndpoint {
				errorResponse = utils.JSON{
					"status":         http.StatusText(statusCode),
					"status_code":    statusCode,
					"reason":         "REFRESH_CAPTCHA",
					"reason_message": "Captcha missing, expired, or already used. Please call /prelogin_captcha to get a new captcha.",
				}
			} else {
				errorResponse = utils.JSON{
					"status":         http.StatusText(statusCode),
					"status_code":    statusCode,
					"reason":         "REFRESH_PREKEY",
					"reason_message": "Prekey missing, expired, or already used. Please call /prelogin to get a new prekey.",
				}
			}
			errorBytes, _ := json.Marshal(errorResponse)

			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(statusCode)
			aepr.ResponseHeaderSent = true
			aepr.ResponseStatusCode = statusCode

			_, err := responseWriter.Write(errorBytes)
			if err != nil {
				aepr.Log.Warnf("ERROR_WRITING_PREKEY_REFRESH_RESPONSE:%v", err)
			}
			aepr.ResponseBodySent = true

			traceMsg := "REFRESH_PREKEY_SENT"
			if isCaptchaEndpoint {
				traceMsg = "REFRESH_CAPTCHA_SENT"
			}
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, traceMsg)
			return
		}

		preKeyIndex, err := utils.GetStringFromKV(aepr.EncryptionParameters, "PRE_KEY_INDEX")
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_GET_PRE_KEY_INDEX:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_GET_PRE_KEY_INDEX")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		aepr.ResponseStatusCode = statusCode

		payLoadStatusCodeAsBytes := make([]byte, 8)
		// --- BIG-ENDIAN ---
		binary.BigEndian.PutUint64(payLoadStatusCodeAsBytes, uint64(statusCode))

		payLoadStatusCodeAsBase64 := base64.StdEncoding.EncodeToString(payLoadStatusCodeAsBytes)

		lvPayLoadStatusCode, err := lv.NewLV([]byte(payLoadStatusCodeAsBase64))
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_NEW_LV_PAYLOAD_STATUS_CODE:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_NEW_LV_PAYLOAD_STATUS_CODE")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		payLoadHeaderAsBytes, err := json.Marshal(header)
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_MARSHAL_PAYLOAD_HEADER_AS_BYTES:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_MARSHAL_PAYLOAD_HEADER_AS_BYTES")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
		payLoadHeaderAsBase64 := base64.StdEncoding.EncodeToString(payLoadHeaderAsBytes)

		lvPayLoadHeader, err := lv.NewLV([]byte(payLoadHeaderAsBase64))
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_NEW_LV_PAYLOAD_HEADER:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_NEW_LV_PAYLOAD_HEADER")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		payLoadBodyAsBase64 := base64.StdEncoding.EncodeToString(bodyAsBytes)

		lvPayLoadBody, err := lv.NewLV([]byte(payLoadBodyAsBase64))
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_NEW_LV_PAYLOAD_BODY:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_NEW_LV_PAYLOAD_BODY")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		edB0PrivateKeyAsBytes, err := utils.GetBytesFromKV(aepr.EncryptionParameters, "ED_B0_PRIVATE_KEY_AS_BYTES")
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_GET_ED_B0_PRIVATE_KEY_AS_BYTES:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_GET_ED_B0_PRIVATE_KEY_AS_BYTES")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
		sharedKey2AsBytes, err := utils.GetBytesFromKV(aepr.EncryptionParameters, "SHARED_KEY_2_AS_BYTES")
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_GET_SHARED_KEY_2_AS_BYTES:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_GET_SHARED_KEY_2_AS_BYTES")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		if OnE2EEPrekeyPack == nil {
			aepr.Log.Errorf(err, "NOT_IMPLEMENTED:OnE2EEPrekeyPack_IS_NIL:%v", aepr.EndPoint.EndPointType)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusUnprocessableEntity, "OnE2EEPrekeyPack_IS_NIL")
			responseWriter.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		dataBlockEnvelopeAsHexString, err := OnE2EEPrekeyPack(aepr, preKeyIndex, edB0PrivateKeyAsBytes, sharedKey2AsBytes, lvPayLoadStatusCode, lvPayLoadHeader, lvPayLoadBody)
		if err != nil {
			aepr.Log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_PACKLVPAYLOAD:%+v\n", err)
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_PACKLVPAYLOAD")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		responseWriter.WriteHeader(statusCode)
		aepr.ResponseStatusCode = statusCode

		aepr.ResponseHeaderSent = true

		rawPayload := utils.JSON{
			"d": dataBlockEnvelopeAsHexString,
		}
		rawBodyAsBytes, err := json.Marshal(rawPayload)
		if aepr.ResponseBodySent {
			_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_BODY_ALREADY_SENT")
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "RESPONSE_BODY_ALREADY_SENT")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
		_, err = responseWriter.Write(rawBodyAsBytes)
		if err != nil {
			_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_WRITE_RESPONSE=%s", err.Error())
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, http.StatusBadRequest, "ERROR_AT_WRITE_RESPONSE")
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		aepr.ResponseBodySent = true
		if statusCode != http.StatusOK {
			if bodyAsBytes != nil {
				aepr._responseErrorAsString = ""
			} else {
				aepr._responseErrorAsString = string(bodyAsBytes)
			}
		}

		// TRACE: response_write_end (E2EE success)
		aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "")

	default:
		for k, v := range header {
			responseWriter.Header().Set(k, v)
		}
		responseWriter.WriteHeader(statusCode)
		aepr.ResponseStatusCode = statusCode

		aepr.ResponseHeaderSent = true
		if aepr.ResponseBodySent {
			_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_BODY_ALREADY_SENT")
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "RESPONSE_BODY_ALREADY_SENT")
			return
		}
		_, err := responseWriter.Write(bodyAsBytes)
		if err != nil {
			_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_WRITE_RESPONSE=%s", err.Error())
			aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "ERROR_AT_WRITE_RESPONSE")
			return
		}
		aepr.ResponseBodySent = true
		if statusCode != http.StatusOK {
			if bodyAsBytes != nil {
				aepr._responseErrorAsString = ""
			} else {
				aepr._responseErrorAsString = string(bodyAsBytes)
			}
		}

		// TRACE: response_write_end (default success)
		aepr.logResponseTrace("response_write_end", responseWriteStartTime, statusCode, "")
	}
	return
}

func (aepr *DXAPIEndPointRequest) NewAPIEndPointRequestParameter(aepp DXAPIEndPointParameter) *DXAPIEndPointRequestParameterValue {
	aerp := DXAPIEndPointRequestParameterValue{Owner: aepr, Metadata: aepp}
	aepr.ParameterValues[aepp.NameId] = &aerp
	return &aerp
}

func (aepr *DXAPIEndPointRequest) PreProcessRequest() (err error) {
	if aepr.EndPoint.RequestMaxContentLength > 0 {
		if aepr.Request.ContentLength > aepr.EndPoint.RequestMaxContentLength {
			return aepr.WriteResponseAndNewErrorf(http.StatusRequestEntityTooLarge, "", "REQUEST_MAX_CONTENT_LENGTH_EXCEEDED:%d<%d", aepr.EndPoint.RequestMaxContentLength, aepr.Request.ContentLength)
		}
	}
	aepr.ParameterValues = map[string]*DXAPIEndPointRequestParameterValue{}
	aepr.CurrentUser = DXAPIUser{}
	aepr.LocalData = map[string]any{}
	aepr.ErrorMessage = []string{}
	aepr.ResponseHeaderSent = false
	aepr.ResponseBodySent = false
	aepr.RequestBodyAsBytes = nil
	if aepr.Request.Method != aepr.EndPoint.Method {
		if aepr.Request.Method == "OPTIONS" {
			aepr.WriteResponseAsBytes(http.StatusOK, nil, []byte(""))
			return nil
		}
		return aepr.WriteResponseAndNewErrorf(http.StatusMethodNotAllowed, "", "METHOD_NOT_ALLOWED:%s!=%s", aepr.Request.Method, aepr.EndPoint.Method)
	}
	xVar := aepr.Request.Header.Get("X-Var")
	var xVarJSON map[string]interface{}
	if xVar != "" {
		err := json.Unmarshal([]byte(xVar), &xVarJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_PARSING_HEADER_X-VAR_AS_JSON: %v", err.Error())
		}
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			variablePath := v.NameId
			err := rpv.SetRawValue(xVarJSON[v.NameId], variablePath)
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
			}
			if (rpv.Metadata.IsMustExist) && (rpv.RawValue == nil) && (!rpv.Metadata.IsNullable) {
				s := fmt.Sprintf("MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, s, s)
			}
			if rpv.RawValue != nil {
				err = rpv.Validate()
				if err != nil {
					aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
					return err
				}
			}
		}
	}
	switch aepr.EndPoint.Method {
	case "GET", "DELETE":
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			variablePath := v.NameId
			err := rpv.SetRawValue(aepr.Request.FormValue(v.NameId), variablePath)
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
			}
			if (rpv.Metadata.IsMustExist) && (rpv.RawValue == nil) && (!rpv.Metadata.IsNullable) {
				s := fmt.Sprintf("MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, s, s)
			}
			if rpv.RawValue != nil {
				err = rpv.Validate()
				if err != nil {
					aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
					return err
				}
			}
		}
	case "POST", "PUT":
		switch aepr.EndPoint.RequestContentType {
		case utilsHttp.RequestContentTypeApplicationOctetStream:
			for _, v := range aepr.EndPoint.Parameters {
				rpv, ok := aepr.ParameterValues[v.NameId]
				variablePath := v.NameId
				if v.IsMustExist {
					if !ok {
						s := fmt.Sprintf("MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, s, s)
					}
				}
				if rpv.RawValue != nil {
					err = rpv.Validate()
					if err != nil {
						aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
						return err
					}
				}
			}
			return aepr.preProcessRequestAsApplicationOctetStream()
		case utilsHttp.RequestContentTypeApplicationJSON:
			return aepr.preProcessRequestAsApplicationJSON()
		default:
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "Request content-type is not supported yet (%v)", aepr.EndPoint.RequestContentType)
		}
	default:
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "Request method is not supported yet (%v)", aepr.EndPoint.Method)
	}
	return nil
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationOctetStream() (err error) {
	switch aepr.EndPoint.EndPointType {
	case EndPointTypeHTTPUploadStream:
		return nil
	case EndPointTypeWS:
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_CONTENT_TYPE_OCTETSTREAM_ENDPOINT_TYPE_X_NOT_IMPLEMENTED_YET:%v", aepr.EndPoint.Method)
	default:
		aepr.RequestBodyAsBytes, err = io.ReadAll(aepr.Request.Body)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_READING_REQUEST_BODY: %v", err.Error())
		}
	}
	return nil
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationJSON() (err error) {
	actualContentType := aepr.Request.Header.Get("Content-Type")
	if actualContentType != "" {
		if !strings.Contains(actualContentType, "application/json") {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_CONTENT_TYPE_IS_NOT_APPLICATION_JSON: %s", actualContentType)
		}
	}
	bodyAsJSON := utils.JSON{}
	aepr.RequestBodyAsBytes, err = io.ReadAll(aepr.Request.Body)
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_BODY_CANT_BE_READ:%v=%v", err.Error(), aepr.RequestBodyAsBytes)
	}

	if len(aepr.RequestBodyAsBytes) > 0 {
		err = json.Unmarshal(aepr.RequestBodyAsBytes, &bodyAsJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_BODY_CANT_BE_PARSED_AS_JSON:%v", err.Error()+"="+string(aepr.RequestBodyAsBytes))
		}
	}

	switch aepr.EndPoint.EndPointType {
	case EndPointTypeHTTPJSON, EndPointTypeHTTPDownloadStream:
		err := aepr.processEndPointRequestParameterValues(bodyAsJSON)
		if err != nil {
			return err
		}
	case EndPointTypeHTTPEndToEndEncryptionV1:
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_CONTENT_TYPE_JSON_ENDPOINT_TYPE_X_NOT_IMPLEMENTED_YET:%v", aepr.EndPoint.Method)
	case EndPointTypeHTTPEndToEndEncryptionV2, EndPointTypeHTTPDownloadStreamV2:
		preKeyIndex, err := utils.GetStringFromKV(bodyAsJSON, "i")
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "INVALID_REQUEST_FORMAT", "INVALID_REQUEST_FORMAT:%v", err)
		}
		dataAsHexString, err := utils.GetStringFromKV(bodyAsJSON, "d")
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "INVALID_REQUEST_FORMAT", "INVALID_REQUEST_FORMAT:%v", err)
		}

		if OnE2EEPrekeyUnPack == nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "NOT_IMPLEMENTED", "NOT_IMPLEMENTED:OnE2EEPrekeyUnPack_IS_NIL:%v", aepr.EndPoint.EndPointType)
		}

		lvPayloadElements, sharedKey2AsBytes, edB0PrivateKeyAsBytes, preKeyData, err := OnE2EEPrekeyUnPack(aepr, preKeyIndex, dataAsHexString)
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "INVALID_PREKEY", "NOT_ERROR:UNPACK_ERROR:%v", err.Error())
		}

		lvPayloadHeader := lvPayloadElements[0]
		lvPayloadBody := lvPayloadElements[1]

		payLoadHeaderAsBase64 := lvPayloadHeader.Value
		payLoadHeaderAsBytes, err := base64.StdEncoding.DecodeString(string(payLoadHeaderAsBase64))
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "DATA_CORRUPT", "DATA_CORRUPT:INVALID_DECODED_PAYLOAD_HEADER_FROM_BASE64")
		}
		payloadHeader := map[string]string{}
		err = json.Unmarshal(payLoadHeaderAsBytes, &payloadHeader)
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "DATA_CORRUPT", "DATA_CORRUPT:INVALID_UNMARSHAL_PAYLOAD_HEADER_BYTES")
		}

		aepr.EncryptionParameters = utils.JSON{
			"PRE_KEY_INDEX":              preKeyIndex,
			"SHARED_KEY_2_AS_BYTES":      sharedKey2AsBytes,
			"ED_B0_PRIVATE_KEY_AS_BYTES": edB0PrivateKeyAsBytes,
			"PRE_KEY_DATA":               preKeyData,
		}
		aepr.EffectiveRequestHeader = payloadHeader

		payLoadBodyAsBase64 := lvPayloadBody.Value
		payLoadBodyAsBytes, err := base64.StdEncoding.DecodeString(string(payLoadBodyAsBase64))
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "DATA_CORRUPT", "DATA_CORRUPT:INVALID_DECODED_PAYLOAD_BODY_FROM_BASE64")
		}
		payloadBodyAsJSON := utils.JSON{}
		err = json.Unmarshal(payLoadBodyAsBytes, &payloadBodyAsJSON)
		if err != nil {
			return aepr.WriteResponseAndLogAsErrorf(http.StatusUnprocessableEntity, "DATA_CORRUPT", "DATA_CORRUPT:INVALID_UNMARSHAL_PAYLOAD_BODY_BYTES")
		}
		err = aepr.processEndPointRequestParameterValues(payloadBodyAsJSON)
		if err != nil {
			return err
		}
	default:
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_CONTENT_TYPE_JSON_ENDPOINT_TYPE_X_NOT_SUPPORTED:%v", aepr.EndPoint.EndPointType)
	}
	return nil

}

func (aepr *DXAPIEndPointRequest) processEndPointRequestParameterValues(bodyAsJSON utils.JSON) (err error) {
	for _, v := range aepr.EndPoint.Parameters {
		rpv := aepr.NewAPIEndPointRequestParameter(v)
		aepr.ParameterValues[v.NameId] = rpv
		variablePath := v.NameId
		err := rpv.SetRawValue(bodyAsJSON[v.NameId], variablePath)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
		}
		if (rpv.Metadata.IsMustExist) && (rpv.RawValue == nil) && (!rpv.Metadata.IsNullable) {
			s := fmt.Sprintf("MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, s, s)
		}
		if rpv.RawValue != nil {
			err = rpv.Validate()
			if err != nil {
				aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
				return err
			}
		}
	}
	return nil
}
