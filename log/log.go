package log

import (
	"context"
	"dxlib/v3/core"
	"fmt"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
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
	DXLogLevelInfo:  "INFO ",
	DXLogLevelWarn:  "WARN ",
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

func NewLog(parentLog *DXLog, context context.Context, prefix string) DXLog {
	if parentLog != nil {
		if parentLog.Prefix != "" {
			prefix = parentLog.Prefix + " | " + prefix
		}
	}
	l := DXLog{Context: context, Prefix: prefix}
	/*if parentLog != nil {
		l.OnLogged = parentLog.OnLogged
	}*/
	return l
}

func (l *DXLog) LogText(severity DXLogLevel, location string, text string) {
	//severityAsString := DXLogLevelAsString[severity]
	/*	switch Format {
		case DXLogFormatJSON:
	*/
	stack := ``
	a := log.WithFields(log.Fields{"prefix": l.Prefix, "location": location})
	switch severity {
	case DXLogLevelTrace:
		a.Tracef("%s", text)
	case DXLogLevelDebug:
		a.Debugf("%s", text)
	case DXLogLevelInfo:
		a.Infof("%s", text)
	case DXLogLevelWarn:
		a.Warnf("%s", text)
	case DXLogLevelError:
		a.Errorf("%s", text)
	case DXLogLevelFatal:
		a.Fatalf("Terminating... %s", text)
	case DXLogLevelPanic:
		stack = string(debug.Stack())
		a = a.WithField(`stack`, stack)
		a.Fatalf("%s", text)
	default:
		a.Printf("%s", text)
	}
	/*if l.OnLogged != nil {
		l.OnLogged(l, severity, location, text, stack)
	}*/
	/*	default:
		switch severity {
		case DXLogLevelTrace:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelDebug:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelInfo:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelWarn:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelError:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelFatal:
			log.Fatalf("[%s] %s| Terminating... %s\n", l.Prefix, severityAsString, text)
		case DXLogLevelPanic:
			stack := string(debug.Stack())
			log.Fatalf("[%s] %s| %s\n%v", l.Prefix, severityAsString, text, stack)
		default:
			log.Printf("[%s] %s| %s\n", l.Prefix, severityAsString, text)
		}
	}*/
}

func (l *DXLog) Trace(text string) {
	l.LogText(DXLogLevelTrace, ``, text)
}

func (l *DXLog) Tracef(text string, v ...any) {
	t := fmt.Sprintf(text, v...)
	l.Trace(t)
}

func (l *DXLog) Debug(text string) {
	l.LogText(DXLogLevelDebug, ``, text)
}

func (l *DXLog) Debugf(text string, v ...any) {
	t := fmt.Sprintf(text, v...)
	l.Debug(t)
}

func (l *DXLog) Info(text string) {
	l.LogText(DXLogLevelInfo, ``, text)
}

func (l *DXLog) Infof(text string, v ...any) {
	t := fmt.Sprintf(text, v...)
	l.Info(t)
}

func (l *DXLog) Warn(text string) {
	l.LogText(DXLogLevelWarn, ``, text)
}

func (l *DXLog) Warnf(text string, v ...any) {
	t := fmt.Sprintf(text, v...)
	l.Warn(t)
}

func (l *DXLog) WarnAndCreateErrorf(text string, v ...any) (err error) {
	err = fmt.Errorf(text, v...)
	l.LogText(DXLogLevelWarn, ``, err.Error())
	return err
}

func (l *DXLog) Error(text string) {
	l.LogText(DXLogLevelError, ``, text)
}

func (l *DXLog) Errorf(text string, v ...any) {
	t := fmt.Sprintf(text, v...)
	l.Error(t)
}

func (l *DXLog) ErrorAndCreateErrorf(text string, v ...any) (err error) {
	err = fmt.Errorf(text, v...)
	l.Error(err.Error())
	return err
}

func (l *DXLog) Fatal(text string) {
	l.LogText(DXLogLevelFatal, ``, text)
}

func (l *DXLog) Fatalf(text string, v ...any) {
	l.Fatal(fmt.Sprintf(text, v...))
}

func (l *DXLog) FatalAndCreateErrorf(text string, v ...any) (err error) {
	err = fmt.Errorf(text, v...)
	l.Fatal(err.Error())
	return err
}

func (l *DXLog) Panic(location string, err error) {
	l.LogText(DXLogLevelPanic, location, err.Error())
}

func (l *DXLog) PanicAndCreateErrorf(location, text string, v ...any) (err error) {
	err = fmt.Errorf(text, v...)
	l.Panic(location, err)
	return err
}

var Log DXLog

func SetFormatJSON() {
	log.SetFormatter(&log.JSONFormatter{})
	Format = DXLogFormatJSON
}

func SetFormatText() {
	log.SetFormatter(&log.TextFormatter{})
	Format = DXLogFormatText
}

func init() {
	//log.SetFlags(log.Ldate | log.Lmicroseconds | log.LUTC)
	//	log.SetReportCaller(true)
	log.SetLevel(log.TraceLevel)
	SetFormatJSON()
	Log = NewLog(nil, core.RootContext, "")
}
