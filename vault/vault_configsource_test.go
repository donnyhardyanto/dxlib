package vault

import "testing"

// Verifies the config-source labels reported by the Get*OrEnvOrDefault fallback helpers.
//
// Why this test exists (BUG-SEC-149): a config key was read under one name while every setter wrote
// another. Nothing set the name the reader looked for, so the value silently came from the compiled
// default. Because that default happened to equal the intended value, behaviour was correct and the
// defect was undetectable. The startup log now reports WHICH TIER supplied each value, so
// "source=DEFAULT" on a key that run.env plainly sets is the bug stated out loud.
//
// The label must therefore be accurate, or the diagnostic is worse than none. The case that is easy
// to get wrong is the third one: an env var that IS set but does not parse falls back to the default,
// so reporting "ENV" there would be a lie.

func TestEnvOrDefaultSource(t *testing.T) {
	const key = "DXLIB_TEST_CFG_STRING"

	if got, src := envOrDefault(key, "fallback"); got != "fallback" || src != cfgSrcDefault {
		t.Errorf("env unset: got (%q, %q), want (%q, %q)", got, src, "fallback", cfgSrcDefault)
	}

	t.Setenv(key, "from-env")
	if got, src := envOrDefault(key, "fallback"); got != "from-env" || src != cfgSrcEnv {
		t.Errorf("env set: got (%q, %q), want (%q, %q)", got, src, "from-env", cfgSrcEnv)
	}

	// An empty env var is treated as unset, matching envOrDefault's documented precedence.
	t.Setenv(key, "")
	if got, src := envOrDefault(key, "fallback"); got != "fallback" || src != cfgSrcDefault {
		t.Errorf("env empty: got (%q, %q), want (%q, %q)", got, src, "fallback", cfgSrcDefault)
	}
}

func TestEnvIntOrDefaultSource(t *testing.T) {
	const key = "DXLIB_TEST_CFG_INT"

	if got, src := envIntOrDefault(key, 7); got != 7 || src != cfgSrcDefault {
		t.Errorf("env unset: got (%d, %q), want (7, %q)", got, src, cfgSrcDefault)
	}

	t.Setenv(key, "42")
	if got, src := envIntOrDefault(key, 7); got != 42 || src != cfgSrcEnv {
		t.Errorf("env set: got (%d, %q), want (42, %q)", got, src, cfgSrcEnv)
	}

	// THE CASE WORTH THE TEST: set but unparseable. The value falls back to the default, so the
	// label must say DEFAULT, not ENV - and it must say WHY, because a typo'd numeric env var is
	// otherwise another silent config failure.
	t.Setenv(key, "not-a-number")
	if got, src := envIntOrDefault(key, 7); got != 7 || src != cfgSrcDefaultUnparsed {
		t.Errorf("env unparseable: got (%d, %q), want (7, %q)", got, src, cfgSrcDefaultUnparsed)
	}
}

func TestEnvInt64OrDefaultSource(t *testing.T) {
	const key = "DXLIB_TEST_CFG_INT64"

	if got, src := envInt64OrDefault(key, 7); got != 7 || src != cfgSrcDefault {
		t.Errorf("env unset: got (%d, %q), want (7, %q)", got, src, cfgSrcDefault)
	}

	t.Setenv(key, "9007199254740993")
	if got, src := envInt64OrDefault(key, 7); got != 9007199254740993 || src != cfgSrcEnv {
		t.Errorf("env set: got (%d, %q), want (9007199254740993, %q)", got, src, cfgSrcEnv)
	}

	t.Setenv(key, "1.5")
	if got, src := envInt64OrDefault(key, 7); got != 7 || src != cfgSrcDefaultUnparsed {
		t.Errorf("env unparseable: got (%d, %q), want (7, %q)", got, src, cfgSrcDefaultUnparsed)
	}
}

func TestEnvBoolOrDefaultSource(t *testing.T) {
	const key = "DXLIB_TEST_CFG_BOOL"

	if got, src := envBoolOrDefault(key, true); got != true || src != cfgSrcDefault {
		t.Errorf("env unset: got (%t, %q), want (true, %q)", got, src, cfgSrcDefault)
	}

	// strconv.ParseBool accepts these; a reader should not have to guess which spellings work.
	for _, s := range []string{"false", "0", "f", "FALSE", "False"} {
		t.Setenv(key, s)
		if got, src := envBoolOrDefault(key, true); got != false || src != cfgSrcEnv {
			t.Errorf("env %q: got (%t, %q), want (false, %q)", s, got, src, cfgSrcEnv)
		}
	}

	// ParseBool does NOT accept these, which surprises people. They must report DEFAULT, not ENV.
	for _, s := range []string{"yes", "no", "on", "off"} {
		t.Setenv(key, s)
		if got, src := envBoolOrDefault(key, true); got != true || src != cfgSrcDefaultUnparsed {
			t.Errorf("env %q: got (%t, %q), want (true, %q)", s, got, src, cfgSrcDefaultUnparsed)
		}
	}
}
