package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"

	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
)

// SanitizeForPostgreSQL replaces null bytes and other invalid UTF-8 sequences
// with their hex representation (e.g., \x00 becomes "0x00") so they can be
// safely stored in PostgreSQL TEXT fields while remaining visible to log viewers.
func SanitizeForPostgreSQL(s string) string {
	if s == "" {
		return s
	}

	var result strings.Builder
	result.Grow(len(s)) // Pre-allocate for common case (no special chars)

	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0x00 {
			// Null byte - show as hex so viewers know it was present
			result.WriteString("0x00")
		} else if b < 0x20 && b != '\n' && b != '\r' && b != '\t' {
			// Other control characters (except newline, carriage return, tab)
			fmt.Fprintf(&result, "0x%02x", b)
		} else {
			result.WriteByte(b)
		}
	}

	return result.String()
}

type DXLogLevel int

const (
	DXLogLevelPanic DXLogLevel = iota // Should be impossible to be wrong... mainly cause by programming error/bug and application must be stopped to prevent giving wrong output
	DXLogLevelFatal                   // Error because wrong configuration or input that prevent the application to continue running or must be exited
	DXLogLevelError                   // Should be impossible to be wrong... mainly cause by programming error/bug, but the application must be to continue running and process other inputs
	DXLogLevelWarn                    // Error because the input, the application must continue running and receive another input
	DXLogLevelInfo
	DXLogLevelDebug
	DXLogLevelTrace
)

var DXLogLevelAsString = map[DXLogLevel]string{
	DXLogLevelTrace: "TRACE",
	DXLogLevelDebug: "DEBUG",
	DXLogLevelInfo:  "INFO",
	DXLogLevelWarn:  "WARN",
	DXLogLevelError: "ERROR",
	DXLogLevelFatal: "FATAL",
	DXLogLevelPanic: "PANIC",
}

type DXLogFormat int

const (
	DXLogFormatText DXLogFormat = iota
	DXLogFormatJSON             = 1
)

type DXLog struct {
	Context         context.Context
	Prefix          string
	RequestURL      string
	LastErrorLogId  int64
	LastErrorLogUid string
}

var Format DXLogFormat
var OnError func(l *DXLog, errPrev error, severity DXLogLevel, location string, text string, stack string) (err error)
var ConsoleLogLevel = LevelTrace // Default: show all logs (backward compatible)

// Custom slog levels for TRACE, FATAL, and PANIC
const (
	LevelTrace slog.Level = slog.LevelDebug - 4
	LevelFatal slog.Level = slog.LevelError + 4
	LevelPanic slog.Level = slog.LevelError + 8
)

func NewLog(parentLog *DXLog, context context.Context, prefix string) DXLog {
	if parentLog != nil {
		if parentLog.Prefix != "" {
			prefix = parentLog.Prefix + " | " + prefix
		}
	}
	l := DXLog{Context: context, Prefix: prefix}
	return l
}

func (l *DXLog) LogText(err error, severity DXLogLevel, location string, text string, v ...any) {
	stack := ""
	if v == nil {
		text = fmt.Sprint(text)
	} else {
		text = fmt.Sprintf(text, v...)
	}

	// Sanitize text to escape null bytes and invalid UTF-8 for PostgreSQL
	text = SanitizeForPostgreSQL(text)

	if err != nil {
		location = l.Prefix
		stack = fmt.Sprintf("%+v", err)
		// Sanitize stack trace as well
		stack = SanitizeForPostgreSQL(stack)
		text = text + "\n" + stack
	}

	attrs := []any{
		slog.String("prefix", l.Prefix),
		slog.String("location", location),
	}

	shouldExit := false

	switch severity {
	case DXLogLevelTrace:
		slog.Log(context.Background(), LevelTrace, text, attrs...)
	case DXLogLevelDebug:
		slog.Debug(text, attrs...)
	case DXLogLevelInfo:
		slog.Info(text, attrs...)
	case DXLogLevelWarn:
		slog.Warn(text, attrs...)
	case DXLogLevelError:
		attrs = append(attrs, slog.String("stack", stack))
		slog.Error(text, attrs...)
	case DXLogLevelFatal:
		slog.Log(context.Background(), LevelFatal, "Terminating... "+text, attrs...)
		shouldExit = true
	case DXLogLevelPanic:
		stack = string(debug.Stack())
		attrs = append(attrs, slog.String("stack", stack))
		slog.Log(context.Background(), LevelPanic, text, attrs...)
		shouldExit = true
	default:
		slog.Info(text, attrs...)
	}

	if OnError != nil {
		err2 := OnError(l, err, severity, location, text, stack)
		if err2 != nil {
			slog.Warn("ERROR_ON_ERROR_HANDLER", slog.Any("error", err2))
		}
	}

	if shouldExit {
		os.Exit(1)
	}
}

func (l *DXLog) Trace(text string) {
	l.LogText(nil, DXLogLevelTrace, l.Prefix, text)
}

func (l *DXLog) Tracef(text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Trace(t)
}

func (l *DXLog) Debug(text string) {
	l.LogText(nil, DXLogLevelDebug, l.Prefix, text)
}

func (l *DXLog) Debugf(text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Debug(t)
}

func (l *DXLog) Info(text string) {
	l.LogText(nil, DXLogLevelInfo, l.Prefix, text)
}

func (l *DXLog) Infof(text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Info(t)
}

func (l *DXLog) Warn(text string) {
	l.LogText(nil, DXLogLevelWarn, l.Prefix, text)
}

func (l *DXLog) Warnf(text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Warn(t)
}

func (l *DXLog) WarnAndCreateError(text string) (err error) {
	err = errors.New(text)
	l.LogText(err, DXLogLevelWarn, "", "")
	return err
}

func (l *DXLog) WarnAndCreateErrorf(text string, v ...any) (err error) {
	err = errors.Errorf(text, v...)
	l.LogText(err, DXLogLevelWarn, "", "")
	return err
}

func (l *DXLog) Error(text string, err error) {
	l.LogText(err, DXLogLevelError, l.Prefix, text)
}

func (l *DXLog) Errorf(err error, text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Error(t, err)
}

func (l *DXLog) ErrorAndCreateErrorf(text string, v ...any) (err error) {
	if v == nil {
		err = errors.Errorf(text)
	} else {
		err = errors.Errorf(text, v...)
	}
	l.Error(err.Error(), err)
	return err
}

func (l *DXLog) Fatal(text string) {
	l.LogText(nil, DXLogLevelFatal, l.Prefix, text)
}

func (l *DXLog) Fatalf(text string, v ...any) {
	t := ""
	if v == nil {
		t = fmt.Sprint(text)
	} else {
		t = fmt.Sprintf(text, v...)
	}
	l.Fatal(t)
}

func (l *DXLog) FatalAndCreateErrorf(text string, v ...any) (err error) {
	if v == nil {
		err = errors.Errorf(text)
	} else {
		err = errors.Errorf(text, v...)
	}
	l.Fatal(err.Error())
	return err
}

func (l *DXLog) Panic(location string, err error) {
	l.LogText(err, DXLogLevelPanic, location, "")
}

func (l *DXLog) PanicAndCreateErrorf(location, text string, v ...any) (err error) {
	if v == nil {
		err = errors.Errorf(text)
	} else {
		err = errors.Errorf(text, v...)
	}
	l.Panic(location, err)
	return err
}

var Log DXLog

func SetFormatJSON() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: ConsoleLogLevel, // Use configurable level instead of hardcoded
	})
	slog.SetDefault(slog.New(handler))
	Format = DXLogFormatJSON
}

func SetFormatText() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: ConsoleLogLevel, // Use configurable level instead of hardcoded
	})
	slog.SetDefault(slog.New(handler))
	Format = DXLogFormatText
}

// SetFormatSimple sets a clean, human-readable format without field labels
// Output: "2026-02-15 18:16:58 Message here"
// Best for CLI tools and human-readable console output
func SetFormatSimple() {
	handler := &simpleHandler{}
	slog.SetDefault(slog.New(handler))
	Format = DXLogFormatText
}

// simpleHandler is a custom slog handler for clean console output
type simpleHandler struct{}

func (h *simpleHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= ConsoleLogLevel
}

func (h *simpleHandler) Handle(ctx context.Context, r slog.Record) error {
	// Format: "2026-02-15 18:16:58 Message"
	timestamp := r.Time.Format("2006-01-02 15:04:05")
	fmt.Fprintf(os.Stdout, "%s %s\n", timestamp, r.Message)
	return nil
}

func (h *simpleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *simpleHandler) WithGroup(name string) slog.Handler {
	return h
}

// SetConsoleLogLevel sets the minimum log level that will be written to console (stdout).
// This does NOT affect the error_log database audit trail, which always captures errors.
// Valid levels: LevelTrace, LevelDebug, LevelInfo, LevelWarn, LevelError
//
// Example: SetConsoleLogLevel(LevelInfo) will only show INFO, WARN, ERROR, FATAL, PANIC on console
func SetConsoleLogLevel(level slog.Level) {
	ConsoleLogLevel = level
	// Re-apply current format with new level
	if Format == DXLogFormatJSON {
		SetFormatJSON()
	} else {
		SetFormatText()
	}
}

// SetConsoleLogLevelFromString sets console log level from string value.
// Valid values: "TRACE", "DEBUG", "INFO", "WARN", "ERROR"
// Invalid/empty values default to INFO
func SetConsoleLogLevelFromString(levelStr string) {
	levelStr = strings.ToUpper(strings.TrimSpace(levelStr))

	var level slog.Level
	switch levelStr {
	case "TRACE":
		level = LevelTrace
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo // Default to INFO for invalid/empty values
	}

	SetConsoleLogLevel(level)
}

func init() {
	SetFormatJSON()
	Log = NewLog(nil, core.RootContext, "")
}
