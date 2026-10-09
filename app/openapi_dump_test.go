package app

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The dump runs Run in a child process, as the RunAndExit tests do, because it
// ends the process. Each hook dump mode must not reach exits with a status of
// its own, so the parent can tell which one ran.

const (
	dumpExitSetVariables = 4
	dumpExitExecute      = 5
	dumpExitAfterStart   = 6
	dumpExitRunReturned  = 3
)

func dumpTestMiddlewareAuth(aepr *api.DXAPIEndPointRequest) error  { return nil }
func dumpTestMiddlewareAudit(aepr *api.DXAPIEndPointRequest) error { return nil }
func dumpTestHandler(aepr *api.DXAPIEndPointRequest) error         { return nil }

// openAPIDumpChild is the child's service: two APIs from an in-code "api"
// configuration, a storage configuration that dump mode must not load, and
// every hook dump mode skips wired to exit.
func openAPIDumpChild(failDefinition bool) {
	Set("openapi-dump-child", "child", "child", true, "", "")
	App.OnDefineConfiguration = func() error {
		configuration.Manager.NewConfiguration("api", "", "", false, false, utils.JSON{
			"public": utils.JSON{"address": "127.0.0.1:0"},
			"admin":  utils.JSON{"address": "127.0.0.1:0"},
		}, nil)
		configuration.Manager.NewConfiguration("storage", "", "", false, false, utils.JSON{
			"db": utils.JSON{"nameid": "db", "is_connect_at_start": true, "must_connected": true},
		}, nil)
		return nil
	}
	App.OnDefineSetVariables = func() error { os.Exit(dumpExitSetVariables); return nil }
	App.OnExecute = func() error { os.Exit(dumpExitExecute); return nil }
	App.OnAfterConfigurationStartAll = func() error { os.Exit(dumpExitAfterStart); return nil }
	App.OnDefineAPIEndPoints = func() error {
		if failDefinition {
			return errors.New("SIMULATED_DEFINITION_FAILURE")
		}
		public := api.Manager.APIs["public"]
		public.NewEndPoint("items", "list items", "/items", "POST", api.EndPointTypeHTTPJSON,
			utilsHttp.RequestContentTypeApplicationJSON, nil, dumpTestHandler, nil, nil,
			[]api.DXAPIEndPointExecuteFunc{dumpTestMiddlewareAuth, dumpTestMiddlewareAudit},
			[]string{"ITEM_READ"}, 0, "")
		public.NewEndPoint("health", "health", "/health", "GET", api.EndPointTypeHTTPJSON,
			utilsHttp.RequestContentTypeNone, nil, dumpTestHandler, nil, nil, nil, nil, 0, "")
		return nil
	}
	// A service that throws Run's error away, the shape RunAndExit exists
	// for: the dump must still report failure through the exit status.
	_ = App.Run()
	os.Exit(dumpExitRunReturned)
}

func runDumpChild(t *testing.T, mode, dir string) (int, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestNothingMatchesThisName")
	cmd.Env = append(os.Environ(), runAndExitChildMarker+"="+mode, OpenAPIDumpEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	t.Fatalf("running the child in mode %q: %v\n%s", mode, err, out)
	return -1, ""
}

func TestOpenAPIDumpWritesEveryAPIAndExits(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "openapi")
	code, out := runDumpChild(t, "openapi-dump", dir)
	switch code {
	case 0:
	case dumpExitSetVariables, dumpExitExecute, dumpExitAfterStart:
		t.Fatalf("dump mode ran a hook it must skip (exit %d)\n%s", code, out)
	case dumpExitRunReturned:
		t.Fatalf("Run returned in dump mode instead of ending the process\n%s", out)
	default:
		t.Fatalf("dump exited %d, want 0\n%s", code, out)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if want := []string{"admin.openapi.json", "public.openapi.json"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("files %v, want %v", names, want)
	}

	b, err := os.ReadFile(filepath.Join(dir, "public.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b)
	}
	got := doc.Paths["/items"]["post"]["x-dxlib-middlewares"]
	want := []any{
		"github.com/donnyhardyanto/dxlib/app.dumpTestMiddlewareAuth",
		"github.com/donnyhardyanto/dxlib/app.dumpTestMiddlewareAudit",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("x-dxlib-middlewares = %v, want %v", got, want)
	}
	if _, present := doc.Paths["/health"]["get"]["x-dxlib-middlewares"]; present {
		t.Errorf("an endpoint with no middlewares carries x-dxlib-middlewares:\n%s", b)
	}

	// The same service dumped again gives the same bytes.
	dir2 := filepath.Join(t.TempDir(), "again")
	if code, out := runDumpChild(t, "openapi-dump", dir2); code != 0 {
		t.Fatalf("second dump exited %d\n%s", code, out)
	}
	b2, err := os.ReadFile(filepath.Join(dir2, "public.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(b2) {
		t.Errorf("two dumps of one service differ:\n%s\n---\n%s", b, b2)
	}
}

func TestOpenAPIDumpFailureExitsNonZeroAndWritesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "openapi")
	code, out := runDumpChild(t, "openapi-dump-fails", dir)
	if code != 1 {
		t.Fatalf("a failed dump exited %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "SIMULATED_DEFINITION_FAILURE") {
		t.Errorf("the reason was not logged:\n%s", out)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("a failed dump left %s behind (stat: %v)", dir, err)
	}
}
