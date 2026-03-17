package log

import (
	"fmt"
	"net/http"
	"net/url"
)

// SetTelegramBot configures Telegram alerting for dxlib/log.
// Messages are sent on WARN/ERROR/FATAL/PANIC severity via raw HTTP POST (zero new dependencies).
// token: Telegram bot token, chatIDs: list of chat IDs to send to.
func SetTelegramBot(token string, chatIDs []string) {
	OnError = func(l *DXLog, errPrev error, severity DXLogLevel, location string, text string, stack string) error {
		if severity > DXLogLevelWarn {
			return nil
		}
		msg := fmt.Sprintf("[%s] %s: %s", DXLogLevelAsString[severity], l.Prefix, text)
		if len(msg) > 4096 {
			msg = msg[:4096]
		}
		for _, chatID := range chatIDs {
			sendTelegramMessage(token, chatID, msg)
		}
		return nil
	}
}

func sendTelegramMessage(token string, chatID string, text string) {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	_, _ = http.PostForm(apiURL, url.Values{
		"chat_id": {chatID},
		"text":    {text},
	})
}
