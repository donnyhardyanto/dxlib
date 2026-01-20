package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/donnyhardyanto/dxlib/core"
	"github.com/pkg/errors"
)

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
	Context context.Context
	Prefix  string
}

var Format DXLogFormat
var OnError func(errPrev error, severity DXLogLevel, location string, text string, stack string) (err error)

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
	if err != nil {
		location = l.Prefix
		stack = fmt.Sprintf("%+v", err)
		text = text + "\n" + err.Error()
	}

	attrs := []any{
		slog.String("prefix", l.Prefix),
		slog.String("location", location),
	}

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
		os.Exit(1)
	case DXLogLevelPanic:
		stack = string(debug.Stack())
		attrs = append(attrs, slog.String("stack", stack))
		slog.Log(context.Background(), LevelPanic, text, attrs...)
		os.Exit(1)
	default:
		slog.Info(text, attrs...)
	}

	if OnError != nil {
		err2 := OnError(err, severity, location, text, stack)
		if err2 != nil {
			slog.Warn("ERROR_ON_ERROR_HANDLER", slog.Any("error", err2))
		}
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
		Level: LevelTrace,
	})
	slog.SetDefault(slog.New(handler))
	Format = DXLogFormatJSON
}

func SetFormatText() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: LevelTrace,
	})
	slog.SetDefault(slog.New(handler))
	Format = DXLogFormatText
}

func init() {
	SetFormatJSON()
	Log = NewLog(nil, core.RootContext, "")
}
