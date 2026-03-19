package log

import (
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
