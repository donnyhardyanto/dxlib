package api

import (
	"crypto/tls"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
)

// TLSPreflightReport runs the TLS preflight over every tls block the process
// is configured with -- each API in the "api" configuration, and the
// "http-client" block -- and returns one text report. dialAddr, when not
// empty, is a "host:port" the client configuration is also asked to dial once;
// when empty, the block's own preflight-dial is used if it has one.
//
// It is the entry point for validating an air-gapped deployment: it reads the
// configured certificate, key and CA files and nothing else, reports each
// certificate's subject, SANs, key, validity window and days remaining beside
// the current clock, checks the leaf chains to the configured CA pool, and
// prints the effective policy. A service wires it to a CLI flag
// (--tls-preflight) or to an OAM route through APIHandlerTLSPreflight.
//
// The report is a description of files and settings, never of a running
// listener. Run it against the same configuration name the service starts
// with, or it describes a deployment that does not exist.
func TLSPreflightReport(dialAddr string) (report string, ok bool) {
	var parts []string
	ok = true
	if apiConfiguration, exists := dxlibConfiguration.Manager.Configurations["api"]; exists && apiConfiguration.Data != nil {
		for name, raw := range *apiConfiguration.Data {
			block, isJSON := raw.(utils.JSON)
			if !isJSON {
				continue
			}
			tlsBlock, present := block["tls"]
			if !present {
				parts = append(parts, "server "+name+": no tls block (plaintext)\n")
				continue
			}
			tlsKV, isJSON := tlsBlock.(utils.JSON)
			if !isJSON {
				parts = append(parts, "server "+name+": tls is not an object\n")
				ok = false
				continue
			}
			r := utilsTLS.PreflightServer(name, tlsKV)
			parts = append(parts, r.Text)
			ok = ok && r.OK
		}
	} else {
		parts = append(parts, "no api configuration\n")
	}
	if clientConfiguration, exists := dxlibConfiguration.Manager.Configurations["http-client"]; exists && clientConfiguration.Data != nil {
		if tlsBlock, present := (*clientConfiguration.Data)["tls"]; present {
			if tlsKV, isJSON := tlsBlock.(utils.JSON); isJSON {
				if dialAddr == "" {
					if settings, err := utilsTLS.ParseClientSettings(tlsKV); err == nil {
						dialAddr = settings.PreflightDial
					}
				}
				r := utilsTLS.PreflightClient(tlsKV, dialAddr)
				parts = append(parts, r.Text)
				ok = ok && r.OK
			} else {
				parts = append(parts, "http-client: tls is not an object\n")
				ok = false
			}
		} else {
			parts = append(parts, "http-client: no tls block (outbound calls are plaintext or verify against nothing this library configured)\n")
		}
	} else {
		parts = append(parts, "http-client: no configuration (outbound clients are the bare default)\n")
	}
	return strings.Join(parts, "\n"), ok
}

// APIHandlerTLSPreflight serves TLSPreflightReport as text/plain, 200 when
// every block passed and 503 otherwise, so a deploy pipeline can gate on the
// status alone.
//
// The dial target is not taken from the request. It comes only from the
// http-client block's preflight-dial key: a route reachable over the network
// that dialled whatever host:port a caller named would be a way to open
// connections from inside the service to addresses of the caller's choosing.
// The CLI path, TLSPreflightReport(dialAddr), may take the address from a flag,
// because whoever runs it already has the shell.
//
// Register it on the OAM API with an explicit HTTP method. In this product
// family an OAM command registered with an empty method panics at call time
// with OAMCommandConfigurationErrorMethodEmpty; a route that exists only to be
// called when something is wrong must not itself be the thing that is wrong.
// This is the shape the services' working ping route uses
// (service-contact-center-queue-scheduler/module_instance/define_oam_system.go),
// with only the title, path and handler changed:
//
//	oam := dxlibAPI.Manager.APIs["oam"]
//	oam.NewEndPoint(
//	    "cmdTLSPreflight", "TLS preflight", "/cmdTLSPreflight", "GET",
//	    dxlibAPI.EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone,
//	    nil, dxlibAPI.APIHandlerTLSPreflight, nil, nil, nil, nil, 0, "",
//	)
func APIHandlerTLSPreflight(aepr *DXAPIEndPointRequest) (err error) {
	report, ok := TLSPreflightReport("")
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	aepr.WriteResponseAsString(status, map[string]string{"Content-Type": "text/plain; charset=utf-8"}, report)
	return nil
}

// tlsAttributes are the metric and span attributes for a request's TLS state:
// negotiated version and suite, and the verified peer identity. With them,
// "is any caller still on 1.2" and "who is calling this route" are dashboard
// queries. Peer identity is bounded by the number of services in the cluster,
// so it is safe as a metric dimension. Empty for a plaintext request.
func tlsAttributes(cs *tls.ConnectionState, peerIdentity string) []attribute.KeyValue {
	if cs == nil {
		return nil
	}
	attrs := []attribute.KeyValue{
		attribute.String("tls.version", tls.VersionName(cs.Version)),
		attribute.String("tls.cipher", tls.CipherSuiteName(cs.CipherSuite)),
	}
	if peerIdentity != "" {
		attrs = append(attrs, attribute.String("tls.peer_identity", peerIdentity))
	}
	return attrs
}
