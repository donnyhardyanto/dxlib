package tls

import (
	"context"
	"crypto/x509"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Certificate expiry is the single most common cause of an mTLS outage, and
// in production it has to be alertable rather than discoverable. Every
// certificate a reloader serves is registered here, and one observable gauge
// reports the days left on each, so "what expires next month" is a dashboard
// query and not a calendar entry.
//
// The gauge is registered against the global OpenTelemetry meter. When the
// process has not configured a provider that meter is a no-op and the
// registration costs nothing; when dxlib/otel installs one later, the global
// API delegates to it, so the order of initialisation does not matter.

type observedCertificate struct {
	role     string // "server" or "client"
	file     string
	subject  string
	notAfter time.Time
}

var (
	observedMu    sync.Mutex
	observed      = map[string]observedCertificate{} // keyed by role+file
	observeOnce   sync.Once
	expiryGauge   metric.Float64ObservableGauge
	observeError  error
	gaugeInitMeta = "dxlib.tls.certificate.expiry"
)

// observeCertificate records a certificate as being in service; it is called
// at load and after every successful reload.
func observeCertificate(role, file string, leaf *x509.Certificate) {
	if leaf == nil {
		return
	}
	observeOnce.Do(registerExpiryGauge)
	observedMu.Lock()
	defer observedMu.Unlock()
	observed[role+":"+file] = observedCertificate{role: role, file: file, subject: leaf.Subject.CommonName, notAfter: leaf.NotAfter}
}

func registerExpiryGauge() {
	meter := otel.Meter("dxlib")
	expiryGauge, observeError = meter.Float64ObservableGauge(gaugeInitMeta,
		metric.WithUnit("d"),
		metric.WithDescription("Days until a TLS certificate in service expires; negative once expired"),
	)
	if observeError != nil {
		return
	}
	_, observeError = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		observedMu.Lock()
		defer observedMu.Unlock()
		now := time.Now()
		for _, c := range observed {
			days := c.notAfter.Sub(now).Hours() / 24
			o.ObserveFloat64(expiryGauge, days, metric.WithAttributes(
				attribute.String("tls.certificate.role", c.role),
				attribute.String("tls.certificate.file", c.file),
				attribute.String("tls.certificate.subject", c.subject),
			))
		}
		return nil
	}, expiryGauge)
}

// ObservedCertificates lists what the gauge is reporting on, for the preflight
// report and for tests.
func ObservedCertificates() []struct {
	Role, File, Subject string
	NotAfter            time.Time
} {
	observedMu.Lock()
	defer observedMu.Unlock()
	out := make([]struct {
		Role, File, Subject string
		NotAfter            time.Time
	}, 0, len(observed))
	for _, c := range observed {
		out = append(out, struct {
			Role, File, Subject string
			NotAfter            time.Time
		}{c.role, c.file, c.subject, c.notAfter})
	}
	return out
}
