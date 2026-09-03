package tls

import (
	"crypto/tls"
	"crypto/x509"
	stderrors "errors"
	"strings"
	"sync"
	"time"

	"github.com/donnyhardyanto/dxlib/log"
)

// A failed handshake produces an error that names the mechanism, not the
// mistake. "certificate has expired or is not yet valid" is, nine times out of
// ten on an air-gapped host, a clock with no NTP; "certificate signed by
// unknown authority" is the wrong CA file mounted; "remote error: tls: bad
// certificate" is the other side rejecting us and telling us nothing about
// why. An operator debugging a deployment needs "your clock is wrong" and
// "your CA is wrong" to be different messages, so both sides classify.

// Handshake failure classes.
const (
	HandshakeClassValidityWindow = "VALIDITY_WINDOW"  // expired or not yet valid: check clocks first
	HandshakeClassTrust          = "TRUST"            // peer does not chain to the configured ca-trust
	HandshakeClassName           = "NAME"             // peer certificate does not carry the name dialled
	HandshakeClassPeerRejectedUs = "PEER_REJECTED_US" // the other side refused our certificate
	HandshakeClassNoClientCert   = "NO_CLIENT_CERT"   // mode=mtls demands a certificate and none came
	HandshakeClassPolicy         = "POLICY"           // no common version, suite or curve
	HandshakeClassIdentity       = "IDENTITY"         // refused by the SAN allow-list
	HandshakeClassRevoked        = "REVOKED"          // a certificate in the peer's chain is on the deny list
	HandshakeClassKeyStrength    = "KEY_STRENGTH"     // refused by the key-size floor
	HandshakeClassTransport      = "TRANSPORT"        // connection dropped mid-handshake; often plaintext on a TLS port
	HandshakeClassOther          = "OTHER"
)

// ClassifyHandshakeError sorts a handshake error into one of the classes
// above, and adds the advice that goes with it. It accepts the typed errors
// crypto/x509 returns on the side that did the verifying, and falls back to
// the text for the side that only received an alert or for a line pulled from
// http.Server's error log.
func ClassifyHandshakeError(err error) (class string, advice string) {
	if err == nil {
		return "", ""
	}
	var invalid x509.CertificateInvalidError
	if stderrors.As(err, &invalid) {
		switch invalid.Reason {
		case x509.Expired:
			return HandshakeClassValidityWindow, validityAdvice()
		case x509.CANotAuthorizedForThisName, x509.NameMismatch:
			return HandshakeClassName, "the peer certificate does not carry the name that was dialled; set server-name to a SAN it does carry, or fix the certificate"
		default:
			return HandshakeClassTrust, "the peer certificate failed chain validation: " + invalid.Error()
		}
	}
	var unknownAuthority x509.UnknownAuthorityError
	if stderrors.As(err, &unknownAuthority) {
		return HandshakeClassTrust, "the peer certificate does not chain to any CA in ca-trust; check that the mounted ca-files are the CA that issued the peer's certificate"
	}
	var hostname x509.HostnameError
	if stderrors.As(err, &hostname) {
		return HandshakeClassName, "the peer certificate does not carry the name that was dialled (" + hostname.Host + "); set server-name to a SAN it does carry"
	}
	var certVerify *tls.CertificateVerificationError
	if stderrors.As(err, &certVerify) && certVerify.Err != nil {
		return ClassifyHandshakeError(certVerify.Err)
	}
	return ClassifyHandshakeText(err.Error())
}

// ClassifyHandshakeText classifies by message text alone. This is what an
// http.Server error-log line offers, and what a client sees when the server
// answered with a bare alert.
func ClassifyHandshakeText(text string) (class string, advice string) {
	t := strings.ToLower(text)
	switch {
	case strings.Contains(t, "expired or is not yet valid"), strings.Contains(t, "not yet valid"), strings.Contains(t, "has expired"):
		return HandshakeClassValidityWindow, validityAdvice()
	case strings.Contains(t, "unknown authority"), strings.Contains(t, "unknown certificate authority"), strings.Contains(t, "not trusted"):
		// "certificate is not trusted" is the macOS platform verifier's
		// wording, which Go passes through as plain text with no type.
		return HandshakeClassTrust, "the certificate does not chain to any CA in ca-trust; check that the mounted ca-files are the CA that issued it"
	case strings.Contains(t, "tls_peer_revoked"):
		// Distinct from TRUST on purpose: the chain is valid, and a
		// certificate in it is on the deny list. The fix is not a CA file.
		return HandshakeClassRevoked, "the peer's certificate chain is valid but a certificate in it is on the deny list (deny-certificates or deny-certificate-signature-algorithms, in the tls block or the deny-file); this refusal is deliberate -- check the deny list before the peer"
	case strings.Contains(t, "tls_peer_not_allowed"):
		return HandshakeClassIdentity, "the peer's certificate is valid but none of its SANs is in allowed-client-sans"
	case strings.Contains(t, "key_too_weak"), strings.Contains(t, "unsupported_key_type"), strings.Contains(t, "is_a_ca_certificate"):
		return HandshakeClassKeyStrength, "the peer's certificate is valid but fails the key-strength floor or is a CA certificate"
	case strings.Contains(t, "client didn't provide a certificate"), strings.Contains(t, "certificate required"):
		return HandshakeClassNoClientCert, "mode=mtls requires a client certificate and the caller sent none; it has no client cert-file configured, or its client is not this library's"
	case strings.Contains(t, "bad certificate"):
		return HandshakeClassPeerRejectedUs, "the other side refused our certificate: on its side, look for TRUST, VALIDITY_WINDOW, IDENTITY, REVOKED or KEY_STRENGTH in its log"
	case strings.Contains(t, "doesn't contain any ip sans"), strings.Contains(t, "not valid for"), strings.Contains(t, "hostname"):
		return HandshakeClassName, "the peer certificate does not carry the name that was dialled; set server-name to a SAN it does carry"
	case strings.Contains(t, "no cipher suite supported by both"), strings.Contains(t, "protocol version not supported"), strings.Contains(t, "unsupported versions"), strings.Contains(t, "no supported elliptic curves"), strings.Contains(t, "handshake failure"):
		return HandshakeClassPolicy, "the peer offered no version, cipher suite or curve that tls-policy allows; the peer needs TLS 1.2 with ECDHE+AEAD or TLS 1.3"
	case strings.Contains(t, "first record does not look like a tls handshake"):
		return HandshakeClassTransport, "the peer spoke plaintext to a TLS port"
	case strings.Contains(t, "eof"), strings.Contains(t, "connection reset"), strings.Contains(t, "broken pipe"):
		return HandshakeClassTransport, "the connection dropped before the handshake finished; a health checker or a plaintext client hitting a TLS port does this"
	default:
		return HandshakeClassOther, ""
	}
}

func validityAdvice() string {
	return "the certificate is outside its validity window at this host's clock (now=" + time.Now().UTC().Format(time.RFC3339) +
		"); on a host without NTP check the clock before the certificate, and compare against the NotBefore/NotAfter the preflight report prints"
}

// handshakeErrorLogWriter is what http.Server.ErrorLog writes into when TLS is
// on. Go reports every failed handshake there as "http: TLS handshake error
// from <addr>: <err>" and nowhere else; without this, the reason a caller was
// refused would go to stderr as an unclassified line. Lines that are not
// handshake errors are passed through at error level.
//
// Warnings are rate-limited per class. This library forwards WARN and ERROR to
// Telegram, and one misconfigured caller retrying every second, or a scanner,
// would otherwise produce a page per handshake. The first refusal in a class is
// logged at once; after that, at most one line per class per
// HandshakeWarnInterval, carrying the count of what was suppressed in between,
// so the operator sees both that it is still happening and how often.
type handshakeErrorLogWriter struct {
	mu   sync.Mutex
	seen map[string]*handshakeWarnState
}

type handshakeWarnState struct {
	lastEmitted time.Time
	suppressed  int
}

// HandshakeWarnInterval is the minimum gap between two warn-level lines for
// the same handshake failure class.
var HandshakeWarnInterval = time.Minute

// NewHandshakeErrorLogWriter returns the writer for http.Server.ErrorLog.
func NewHandshakeErrorLogWriter() *handshakeErrorLogWriter {
	return &handshakeErrorLogWriter{seen: map[string]*handshakeWarnState{}}
}

func (w *handshakeErrorLogWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if !strings.Contains(line, "TLS handshake error") {
		log.Log.Error(line, nil)
		return len(p), nil
	}
	class, advice := ClassifyHandshakeText(line)
	if class == HandshakeClassTransport {
		// A health checker opening and closing a TCP connection produces one
		// of these per probe; at warn level it would drown everything.
		log.Log.Debugf("TLS_HANDSHAKE_REJECTED:%s:%s:%s", class, line, advice)
		return len(p), nil
	}
	w.mu.Lock()
	state, ok := w.seen[class]
	if !ok {
		state = &handshakeWarnState{}
		w.seen[class] = state
	}
	now := time.Now()
	emit := state.lastEmitted.IsZero() || now.Sub(state.lastEmitted) >= HandshakeWarnInterval
	suppressed := state.suppressed
	if emit {
		state.lastEmitted = now
		state.suppressed = 0
	} else {
		state.suppressed++
	}
	w.mu.Unlock()
	if !emit {
		log.Log.Debugf("TLS_HANDSHAKE_REJECTED:%s:%s:%s", class, line, advice)
		return len(p), nil
	}
	if suppressed > 0 {
		log.Log.Warnf("TLS_HANDSHAKE_REJECTED:%s:%s:%s (and %d more of this class in the last %s, logged at debug)", class, line, advice, suppressed, HandshakeWarnInterval)
	} else {
		log.Log.Warnf("TLS_HANDSHAKE_REJECTED:%s:%s:%s", class, line, advice)
	}
	return len(p), nil
}
