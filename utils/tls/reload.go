package tls

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
)

// Certificates rotate; processes should not have to. cert-manager and a bank
// PKI issue short-lived leaves, and Kubernetes rewrites a mounted Secret in
// place (an atomic swap of the ..data symlink), so a process that parsed its
// certificate once at startup serves an expired one until somebody restarts
// the pod. The two reloaders below are what tls.Config.GetCertificate,
// GetClientCertificate and GetConfigForClient call instead of reading a fixed
// field: on every handshake they stat the files and re-read only when the
// modification time or size has moved.
//
// The hot path is a stat, not a read. A stat is a few microseconds against a
// handshake that costs milliseconds of asymmetric crypto, so there is no
// throttle -- one less thing to get wrong. os.Stat follows the symlink, so the
// Kubernetes swap shows up as a new mtime on the target.
//
// A failed reload keeps the previous certificate. A half-written file, a key
// that does not match its certificate, or a rotated leaf that fails the
// strength floor must not take a serving process down; it logs a warning
// (once per distinct problem, not once per handshake) and carries on with what
// it had, which is still valid until its own NotAfter.

// fileStamp is what changes when a file is rewritten: mtime and size together,
// because a filesystem with coarse timestamps can rewrite within one tick.
type fileStamp struct {
	modTime time.Time
	size    int64
}

func stampOf(path string) (fileStamp, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{modTime: info.ModTime(), size: info.Size()}, nil
}

// DXCertificateReloader serves one leaf certificate and its key from a pair of
// PEM files, reloading when either file changes on disk.
type DXCertificateReloader struct {
	CertFile string
	KeyFile  string

	// Check runs against a freshly parsed certificate before it replaces the
	// current one; the policy's key-strength floor goes here. A rejected
	// certificate is logged and the previous one stays in service.
	Check func(leaf *x509.Certificate) error

	// OnReload is told about every successful swap, for the expiry gauge.
	OnReload func(leaf *x509.Certificate)

	mu          sync.Mutex
	cert        *tls.Certificate
	certStamp   fileStamp
	keyStamp    fileStamp
	lastWarning string
}

// NewCertificateReloader loads the pair once, synchronously, so an unreadable
// or mismatched pair is a startup error and not a first-handshake surprise.
func NewCertificateReloader(certFile, keyFile string, check func(*x509.Certificate) error) (*DXCertificateReloader, error) {
	r := &DXCertificateReloader{CertFile: certFile, KeyFile: keyFile, Check: check}
	cert, certStamp, keyStamp, err := r.load()
	if err != nil {
		return nil, err
	}
	r.cert, r.certStamp, r.keyStamp = cert, certStamp, keyStamp
	return r, nil
}

func (r *DXCertificateReloader) load() (cert *tls.Certificate, certStamp, keyStamp fileStamp, err error) {
	certStamp, err = stampOf(r.CertFile)
	if err != nil {
		return nil, certStamp, keyStamp, &DXConfigError{Key: "cert-file", Detail: "CANNOT_STAT:" + r.CertFile + ":" + err.Error()}
	}
	keyStamp, err = stampOf(r.KeyFile)
	if err != nil {
		return nil, certStamp, keyStamp, &DXConfigError{Key: "key-file", Detail: "CANNOT_STAT:" + r.KeyFile + ":" + err.Error()}
	}
	pair, err := tls.LoadX509KeyPair(r.CertFile, r.KeyFile)
	if err != nil {
		return nil, certStamp, keyStamp, &DXConfigError{Key: "cert-file", Detail: "CANNOT_LOAD_KEY_PAIR:" + r.CertFile + "," + r.KeyFile + ":" + err.Error()}
	}
	// LoadX509KeyPair has populated Leaf since Go 1.23; the parse below is for
	// the toolchains before that and costs nothing on the ones after.
	if pair.Leaf == nil {
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return nil, certStamp, keyStamp, &DXConfigError{Key: "cert-file", Detail: "CANNOT_PARSE_LEAF:" + r.CertFile + ":" + err.Error()}
		}
		pair.Leaf = leaf
	}
	if r.Check != nil {
		if err := r.Check(pair.Leaf); err != nil {
			return nil, certStamp, keyStamp, &DXConfigError{Key: "cert-file", Detail: err.Error()}
		}
	}
	return &pair, certStamp, keyStamp, nil
}

// Get returns the current certificate, reloading first if either file changed.
// It never returns an error once construction has succeeded: on any reload
// problem it returns what it already has.
func (r *DXCertificateReloader) Get() (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	certStamp, err1 := stampOf(r.CertFile)
	keyStamp, err2 := stampOf(r.KeyFile)
	if err1 != nil || err2 != nil {
		r.warnOnce("TLS_CERTIFICATE_RELOAD_STAT_FAILED:%s,%s:%v,%v:KEEPING_CURRENT_CERTIFICATE", r.CertFile, r.KeyFile, err1, err2)
		return r.cert, nil
	}
	if certStamp == r.certStamp && keyStamp == r.keyStamp {
		return r.cert, nil
	}
	cert, certStamp, keyStamp, err := r.load()
	if err != nil {
		r.warnOnce("TLS_CERTIFICATE_RELOAD_FAILED:%v:KEEPING_CURRENT_CERTIFICATE", err)
		return r.cert, nil
	}
	r.cert, r.certStamp, r.keyStamp = cert, certStamp, keyStamp
	r.lastWarning = ""
	log.Log.Infof("TLS_CERTIFICATE_RELOADED:%s:subject=%q serial=%s not-after=%s",
		r.CertFile, cert.Leaf.Subject.CommonName, cert.Leaf.SerialNumber.String(), cert.Leaf.NotAfter.UTC().Format(time.RFC3339))
	if r.OnReload != nil {
		r.OnReload(cert.Leaf)
	}
	return r.cert, nil
}

// Leaf is the parsed certificate currently in service.
func (r *DXCertificateReloader) Leaf() *x509.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert.Leaf
}

// GetCertificate is the tls.Config.GetCertificate hook for a server.
func (r *DXCertificateReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.Get()
}

// GetClientCertificate is the tls.Config.GetClientCertificate hook for a
// client. The server's list of acceptable CAs in the request is not consulted:
// there is one certificate to offer, and the server verifies it either way.
func (r *DXCertificateReloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return r.Get()
}

func (r *DXCertificateReloader) warnOnce(format string, args ...any) {
	msg := errors.Errorf(format, args...).Error()
	if msg == r.lastWarning {
		return
	}
	r.lastWarning = msg
	log.Log.Warn(msg)
}

// DXCAPoolReloader serves a trust pool built from a ca-trust mode and its
// files, rebuilding when any file changes.
//
// The system store, when the mode includes it, is loaded once. crypto/x509
// itself loads it once per process and hands out copies, so there is nothing
// to re-read; a change to the host store needs a restart regardless of what
// this type does, and saying so is more honest than pretending otherwise.
type DXCAPoolReloader struct {
	Mode  string
	Files []string

	// Check runs against every CA certificate loaded from a file; the policy's
	// key-strength floor goes here.
	Check func(ca *x509.Certificate) error

	mu          sync.Mutex
	pool        *x509.CertPool
	certs       []*x509.Certificate
	count       int
	stamps      []fileStamp
	lastWarning string
}

// NewCAPoolReloader builds the pool once, synchronously; an unreadable file, a
// file with no certificate in it, or an empty system store is a startup error.
func NewCAPoolReloader(mode string, files []string, check func(*x509.Certificate) error) (*DXCAPoolReloader, error) {
	r := &DXCAPoolReloader{Mode: mode, Files: files, Check: check}
	pool, certs, count, stamps, err := r.load()
	if err != nil {
		return nil, err
	}
	r.pool, r.certs, r.count, r.stamps = pool, certs, count, stamps
	return r, nil
}

func (r *DXCAPoolReloader) load() (pool *x509.CertPool, certs []*x509.Certificate, count int, stamps []fileStamp, err error) {
	stamps = make([]fileStamp, len(r.Files))
	for i, f := range r.Files {
		stamps[i], err = stampOf(f)
		if err != nil {
			return nil, nil, 0, nil, &DXConfigError{Key: "ca-files[" + itoa(i) + "]", Detail: "CANNOT_STAT:" + f + ":" + err.Error()}
		}
	}
	pool, certs, count, err = buildPool(r.Mode, r.Files)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	if r.Check != nil {
		for _, c := range certs {
			if err := r.Check(c); err != nil {
				return nil, nil, 0, nil, &DXConfigError{Key: "ca-files", Detail: err.Error()}
			}
		}
	}
	return pool, certs, count, stamps, nil
}

// Get returns the current pool and whether it differs from the last call's.
// changed is what lets a server's GetConfigForClient hand back nil -- keep the
// config in use -- on the common path and clone only when something moved.
func (r *DXCAPoolReloader) Get() (pool *x509.CertPool, changed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, f := range r.Files {
		stamp, err := stampOf(f)
		if err != nil {
			r.warnOnce("TLS_CA_POOL_RELOAD_STAT_FAILED:%s:%v:KEEPING_CURRENT_POOL", f, err)
			return r.pool, false
		}
		if stamp != r.stamps[i] {
			changed = true
		}
	}
	if !changed {
		return r.pool, false
	}
	pool, certs, count, stamps, err := r.load()
	if err != nil {
		r.warnOnce("TLS_CA_POOL_RELOAD_FAILED:%v:KEEPING_CURRENT_POOL", err)
		return r.pool, false
	}
	r.pool, r.certs, r.count, r.stamps = pool, certs, count, stamps
	r.lastWarning = ""
	log.Log.Infof("TLS_CA_POOL_RELOADED:ca-trust=%s ca-certs=%d files=%v", r.Mode, count, r.Files)
	return r.pool, true
}

// Pool is the pool currently in service, without a reload check.
func (r *DXCAPoolReloader) Pool() *x509.CertPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pool
}

// Count is how many certificates the files contributed. The system store is
// not counted; see buildPool.
func (r *DXCAPoolReloader) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Certificates are the CA certificates loaded from files, for the preflight
// report.
func (r *DXCAPoolReloader) Certificates() []*x509.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*x509.Certificate(nil), r.certs...)
}

func (r *DXCAPoolReloader) warnOnce(format string, args ...any) {
	msg := errors.Errorf(format, args...).Error()
	if msg == r.lastWarning {
		return
	}
	r.lastWarning = msg
	log.Log.Warn(msg)
}

// DXDenyFileReloader serves the deny-list in a deny-file, reloading when the
// file changes. It is the same stat-per-handshake shape as the other two, and
// it is what makes incident response a file push: on the server,
// GetConfigForClient hands back a config with the new suite and curve lists
// and VerifyConnection reads the new certificate denies, so a deny takes
// effect on the next handshake with no restart -- where nginx needs -s reload
// and Kubernetes a rolling restart.
//
// A file that fails to parse, names a token outside the vocabulary, or would
// leave the policy with nothing to offer keeps the previous list in force and
// warns once, like a broken certificate rotation. The warning goes through
// log.Log.Warn, which this library forwards to Telegram, so a typo in a pushed
// deny-file is loud even though it does not take the service down.
type DXDenyFileReloader struct {
	Path string

	// Check runs against a freshly parsed list before it replaces the current
	// one; the policy's "does this leave any suite" test goes here.
	Check func(list *DXDenyList) error

	// OnReload is told about every successful swap, with the list that is now
	// in force.
	OnReload func(list *DXDenyList)

	mu          sync.Mutex
	list        *DXDenyList
	stamp       fileStamp
	lastWarning string
}

// NewDenyFileReloader loads the file once, synchronously; an unreadable or
// malformed file, or one that would empty the policy, is a startup error.
func NewDenyFileReloader(path string, check func(*DXDenyList) error) (*DXDenyFileReloader, error) {
	r := &DXDenyFileReloader{Path: path, Check: check}
	list, stamp, err := r.load()
	if err != nil {
		return nil, err
	}
	r.list, r.stamp = list, stamp
	return r, nil
}

func (r *DXDenyFileReloader) load() (*DXDenyList, fileStamp, error) {
	stamp, err := stampOf(r.Path)
	if err != nil {
		return nil, stamp, &DXConfigError{Key: keyDenyFile, Detail: "CANNOT_STAT:" + r.Path + ":" + err.Error()}
	}
	list, err := loadDenyFile(r.Path)
	if err != nil {
		return nil, stamp, err
	}
	if r.Check != nil {
		if err := r.Check(list); err != nil {
			if ce, ok := err.(*DXConfigError); ok {
				return nil, stamp, &DXConfigError{Key: keyDenyFile, Detail: r.Path + ":" + ce.Key + ":" + ce.Detail}
			}
			return nil, stamp, &DXConfigError{Key: keyDenyFile, Detail: r.Path + ":" + err.Error()}
		}
	}
	return list, stamp, nil
}

// Get returns the current list and whether it differs from the last call's.
func (r *DXDenyFileReloader) Get() (list *DXDenyList, changed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stamp, err := stampOf(r.Path)
	if err != nil {
		r.warnOnce("TLS_DENY_FILE_RELOAD_STAT_FAILED:%s:%v:KEEPING_CURRENT_DENY_LIST", r.Path, err)
		return r.list, false
	}
	if stamp == r.stamp {
		return r.list, false
	}
	list, stamp, err = r.load()
	if err != nil {
		r.warnOnce("TLS_DENY_FILE_RELOAD_FAILED:%v:KEEPING_CURRENT_DENY_LIST:[%s]", err, r.list.Summary())
		return r.list, false
	}
	r.list, r.stamp = list, stamp
	r.lastWarning = ""
	log.Log.Infof("TLS_DENY_FILE_RELOADED:%s:[%s]", r.Path, list.Summary())
	if r.OnReload != nil {
		r.OnReload(list)
	}
	return r.list, true
}

// List is the list currently in service, without a reload check.
func (r *DXDenyFileReloader) List() *DXDenyList {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.list
}

func (r *DXDenyFileReloader) warnOnce(format string, args ...any) {
	msg := errors.Errorf(format, args...).Error()
	if msg == r.lastWarning {
		return
	}
	r.lastWarning = msg
	log.Log.Warn(msg)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
