package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SetFileLogger enables daily rotating file logging to the given directory.
// Call BEFORE SetTelegramBot so the Telegram handler wraps the file handler.
// All log levels are written to file; existing OnError handler is chained.
func SetFileLogger(directory string) error {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return fmt.Errorf("SetFileLogger: mkdir %s: %w", directory, err)
	}

	prevHandler := OnError // chain any previously registered handler

	OnError = func(l *DXLog, errPrev error, severity DXLogLevel, location string, text string, stack string) error {
		filename := fmt.Sprintf("app-%s.log", time.Now().Format("2006-01-02"))
		f, err := os.OpenFile(filepath.Join(directory, filename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			line := fmt.Sprintf("[%s] %s %s: %s\n",
				time.Now().Format("2006-01-02 15:04:05"),
				DXLogLevelAsString[severity],
				l.Prefix,
				text,
			)
			_, _ = f.WriteString(line)
			_ = f.Close()
		}
		if prevHandler != nil {
			return prevHandler(l, errPrev, severity, location, text, stack)
		}
		return nil
	}
	return nil
}

// SetJSONLFileLogger enables a second, machine-readable log stream: one JSON
// object per line in app-<date>.jsonl, rotated daily like SetFileLogger.
//
// It is independent of SetFileLogger -- either, both or neither may be set --
// and chains onto whatever handler is already registered, so the order of the
// Set*Logger calls only decides the order the streams are written in. Call the
// pair of them before SetTelegramBot so Telegram wraps both.
func SetJSONLFileLogger(directory string) error {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return fmt.Errorf("SetJSONLFileLogger: mkdir %s: %w", directory, err)
	}

	prevHandler := OnError

	OnError = func(l *DXLog, errPrev error, severity DXLogLevel, location string, text string, stack string) error {
		filename := fmt.Sprintf("app-%s.jsonl", time.Now().Format("2006-01-02"))
		f, err := os.OpenFile(filepath.Join(directory, filename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			record := map[string]any{
				"time":     time.Now().Format(time.RFC3339Nano),
				"level":    DXLogLevelAsString[severity],
				"prefix":   l.Prefix,
				"location": location,
				"message":  text,
			}
			if stack != "" {
				record["stack"] = stack
			}
			// A record that will not marshal must not cost us the line, so fall
			// back to a minimal one carrying the message as a plain string.
			line, errMarshal := json.Marshal(record)
			if errMarshal != nil {
				line, _ = json.Marshal(map[string]any{
					"time":    time.Now().Format(time.RFC3339Nano),
					"level":   DXLogLevelAsString[severity],
					"message": fmt.Sprint(text),
				})
			}
			_, _ = f.Write(append(line, '\n'))
			_ = f.Close()
		}
		if prevHandler != nil {
			return prevHandler(l, errPrev, severity, location, text, stack)
		}
		return nil
	}
	return nil
}
