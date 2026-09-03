package app

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/errors"
)

// The bug this covers is not in Run, which returns and logs the error
// correctly. It is that every service discarded it with `_ = App.Run()`, so a
// service that could not read its configuration exited 0 and every supervisor,
// probe and CI step read that as success.
//
// os.Exit cannot be observed from inside the process that calls it, so the
// child re-executes this test binary with a marker in the environment and the
// parent inspects the status. This mirrors the child-process pattern already
// used for the fatal-configuration test in the api package.

const runAndExitChildMarker = "DXLIB_TEST_RUN_AND_EXIT_CHILD"

func TestMain(m *testing.M) {
	switch os.Getenv(runAndExitChildMarker) {
	case "define-fails":
		// A service whose configuration or definition step fails. This is the
		// shape of the real failure: a missing config file surfaces through
		// OnDefineConfiguration.
		Set("run-and-exit-child", "child", "child", false, "", "")
		App.OnDefineConfiguration = func() error {
			return errors.New("SIMULATED_CONFIGURATION_FAILURE")
		}
		App.RunAndExit()
		// Reached only if RunAndExit failed to exit on an error, which is the
		// regression. Leave a distinguishable status rather than 0, so the
		// parent cannot mistake it for a pass.
		os.Exit(3)
	case "define-succeeds":
		// Nothing to start: no APIs, no databases, no loop. Run should return
		// nil and RunAndExit should return rather than exiting non-zero.
		Set("run-and-exit-child-ok", "child", "child", false, "", "")
		App.RunAndExit()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runChild(t *testing.T, mode string) (exitCode int, output string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestNothingMatchesThisName")
	cmd.Env = append(os.Environ(), runAndExitChildMarker+"="+mode)
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

// TestRunAndExitLeavesANonZeroStatusOnFailure is the regression: a service that
// fails to start must not look like one that finished.
func TestRunAndExitLeavesANonZeroStatusOnFailure(t *testing.T) {
	code, out := runChild(t, "define-fails")
	if code == 0 {
		t.Fatalf("a failed start exited 0, which every supervisor reads as success\n%s", out)
	}
	if code == 3 {
		t.Fatalf("RunAndExit returned instead of exiting on an error\n%s", out)
	}
	if code != 1 {
		t.Errorf("exited %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "SIMULATED_CONFIGURATION_FAILURE") {
		t.Errorf("the reason was not logged before the process went:\n%s", out)
	}
}

// TestRunAndExitIsSilentOnSuccess pins the other half. If this exited non-zero
// the fix would break every clean shutdown, which is a worse bug than the one
// it repairs.
func TestRunAndExitIsSilentOnSuccess(t *testing.T) {
	code, out := runChild(t, "define-succeeds")
	if code != 0 {
		t.Errorf("a clean run exited %d, want 0\n%s", code, out)
	}
}
