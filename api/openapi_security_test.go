package api

import (
	"encoding/json"
	"strings"
	"testing"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The security section of a document has one job: say something true about the
// transport. mutualTLS is the only scheme OpenAPI 3.1 offers that dxlib can
// actually claim, and it is only true under mode: mtls.

func securityTestAPI(t *testing.T, nameId string) *DXAPI {
	t.Helper()
	a, err := Manager.NewAPI(nameId)
	if err != nil {
		t.Fatalf("NewAPI: %v", err)
	}
	t.Cleanup(func() { delete(Manager.APIs, nameId) })
	a.NewEndPoint("ping", "Ping", "/cmdPing", "POST",
		EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		nil, func(aepr *DXAPIEndPointRequest) error { return nil },
		nil, nil, nil, nil, 0, "")
	return a
}

// TestSecurityIsAbsentWithoutMutualTLS covers the two modes that have nothing
// to declare, plus a plaintext API. A document that named mutualTLS here would
// assert a requirement the listener does not enforce.
func TestSecurityIsAbsentWithoutMutualTLS(t *testing.T) {
	a := securityTestAPI(t, "sec-plain")
	doc, err := a.OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument: %v", err)
	}
	if len(doc.Security) != 0 {
		t.Errorf("plaintext API declared security %v", doc.Security)
	}
	if doc.Components != nil && doc.Components.SecuritySchemes != nil {
		t.Error("plaintext API declared a security scheme")
	}
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatalf("OpenAPIAsJSON: %v", err)
	}
	if strings.Contains(string(b), "mutualTLS") {
		t.Error("mutualTLS appears in a document for an API with no TLS")
	}
}

// TestMutualTLSSchemeRoundTrips is the rule the whole reader rests on: the
// emitter defines the dialect, so anything it produces must read back.
func TestMutualTLSSchemeRoundTrips(t *testing.T) {
	// Build the document an mtls API would emit, by hand rather than by
	// standing up a listener: this test is about the document, and the
	// emitter's own gate on a.TLS is covered by TestSecurityIsAbsentWithoutMutualTLS
	// and by the mTLS handshake tests in utils/tls.
	a := securityTestAPI(t, "sec-mtls")
	doc, err := a.OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument: %v", err)
	}
	name, scheme := OpenAPIMutualTLSSchemeName, &DXOpenAPISecurityScheme{
		Type:        "mutualTLS",
		Description: "Every caller presents a client certificate the listener verifies against its configured CA trust.",
	}
	doc.Components = &DXOpenAPIComponents{SecuritySchemes: NewDXOpenAPIOrderedMap[*DXOpenAPISecurityScheme]()}
	doc.Components.SecuritySchemes.Set(name, scheme)
	doc.Security = []DXOpenAPISecurityRequirement{{name: {}}}

	first, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(first), `"type": "mutualTLS"`) {
		t.Fatalf("emitted document has no mutualTLS scheme:\n%s", first)
	}

	back, err := ReadOpenAPI(first)
	if err != nil {
		t.Fatalf("reading back a document this package emitted: %v", err)
	}
	if len(back.Security) != 1 {
		t.Fatalf("security requirements survived as %v", back.Security)
	}
	scopes, ok := back.Security[0][OpenAPIMutualTLSSchemeName]
	if !ok {
		t.Fatalf("mutualTLS requirement lost: %v", back.Security[0])
	}
	if len(scopes) != 0 {
		t.Errorf("mutualTLS came back with scopes %v; it takes none", scopes)
	}
	if back.Components == nil || back.Components.SecuritySchemes == nil {
		t.Fatal("securitySchemes lost on read")
	}
	got, ok := back.Components.SecuritySchemes.Get(OpenAPIMutualTLSSchemeName)
	if !ok || got.Type != "mutualTLS" {
		t.Fatalf("scheme came back as %+v", got)
	}

	second, err := json.MarshalIndent(back, "", "  ")
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(first) != string(second) {
		t.Errorf("emit -> read -> emit is not lossless for security\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

// TestUnsupportedSecuritySchemesAreRefusedByName is the honesty rule. Each of
// these describes an authentication mechanism dxlib does not enforce, and a
// reader that ignored them would treat the document as a guarantee.
func TestUnsupportedSecuritySchemesAreRefusedByName(t *testing.T) {
	for _, c := range []struct {
		name   string
		scheme string
		expect string
	}{
		{"apiKey in a header", `{"type":"apiKey","in":"header","name":"X-Token"}`, "apiKey_CANNOT_DESCRIBE_A_BODY_TOKEN"},
		{"http bearer", `{"type":"http","scheme":"bearer"}`, "security-scheme-type-http"},
		{"oauth2", `{"type":"oauth2","flows":{}}`, "security-scheme-type-oauth2"},
		{"openIdConnect", `{"type":"openIdConnect","openIdConnectUrl":"https://example.test"}`, "security-scheme-type-openIdConnect"},
	} {
		doc := `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{},` +
			`"components":{"securitySchemes":{"mutualTLS":` + c.scheme + `}}}`
		_, err := ReadOpenAPI([]byte(doc))
		if err == nil {
			t.Errorf("%s was accepted", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.expect) {
			t.Errorf("%s: message does not name the construct: %v", c.name, err)
		}
		if !strings.Contains(err.Error(), "/components/securitySchemes/") {
			t.Errorf("%s: message carries no JSON pointer: %v", c.name, err)
		}
	}
}

// TestSecurityScopesAreRefused keeps privileges and authentication apart.
// A scope array here would be an authorization claim dxlib enforces nothing
// from; authorization is x-dxlib-privileges.
func TestSecurityScopesAreRefused(t *testing.T) {
	doc := `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{},` +
		`"components":{"securitySchemes":{"mutualTLS":{"type":"mutualTLS"}}},` +
		`"security":[{"mutualTLS":["contactcenter-session-list"]}]}`
	_, err := ReadOpenAPI([]byte(doc))
	if err == nil {
		t.Fatal("a scope array was accepted")
	}
	if !strings.Contains(err.Error(), "mutualTLS_TAKES_NO_SCOPES") {
		t.Errorf("unhelpful message: %v", err)
	}
	if !strings.Contains(err.Error(), OpenAPIExtensionPrivileges) {
		t.Errorf("message does not point at the authorization mechanism: %v", err)
	}
}

// TestSecurityRequirementNeedsADeclaredScheme catches a document that requires
// a scheme nothing defines -- readable as OpenAPI, meaningless as a contract.
func TestSecurityRequirementNeedsADeclaredScheme(t *testing.T) {
	doc := `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{},` +
		`"security":[{"mutualTLS":[]}]}`
	_, err := ReadOpenAPI([]byte(doc))
	if err == nil {
		t.Fatal("an undeclared security requirement was accepted")
	}
	if !strings.Contains(err.Error(), "OPENAPI_SECURITY_SCHEME_NOT_DECLARED") {
		t.Errorf("unhelpful message: %v", err)
	}
}

// TestSecuritySchemeNameIsFixed protects the round trip: the emitter always
// writes the key "mutualTLS", so a document using another key for the same
// scheme would not survive emit -> read -> emit.
func TestSecuritySchemeNameIsFixed(t *testing.T) {
	doc := `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{},` +
		`"components":{"securitySchemes":{"mtls":{"type":"mutualTLS"}}}}`
	_, err := ReadOpenAPI([]byte(doc))
	if err == nil {
		t.Fatal("an alternative scheme name was accepted")
	}
	if !strings.Contains(err.Error(), "MUST_BE_NAMED") {
		t.Errorf("unhelpful message: %v", err)
	}
}
