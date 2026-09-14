package stub

import (
	"fmt"
	"log/slog"
	"net/smtp"

	"in.dmart.pulse.conflux/internal/config"
)

// EmailNotifier sends support notifications.
//
// Stubbed while EMAIL_ENABLED=false (Constitution Principle V): the message is
// logged instead of sent, so no test reaches a real mail server. The enabled
// path is a plain SMTP send with no auth, matching the local stack.
type EmailNotifier struct {
	cfg config.Anomaly
	log *slog.Logger
}

// NewEmailNotifier builds the notifier.
func NewEmailNotifier(cfg config.Anomaly, log *slog.Logger) *EmailNotifier {
	return &EmailNotifier{cfg: cfg, log: log}
}

// Notify delivers one notification to support.
func (e *EmailNotifier) Notify(subject, body string) error {
	if !e.cfg.EmailEnabled {
		e.log.Warn("support notification (email disabled; not sent)",
			slog.String("component", "notify"),
			slog.String("to", e.cfg.SupportTo),
			slog.String("subject", subject),
			slog.String("body", body))
		return nil
	}

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: [pulse-conflux] %s\r\n\r\n%s\r\n",
		e.cfg.SupportFrom, e.cfg.SupportTo, subject, body)

	if err := smtp.SendMail(e.cfg.SMTPAddr, nil, e.cfg.SupportFrom,
		[]string{e.cfg.SupportTo}, []byte(msg)); err != nil {
		return fmt.Errorf("smtp send to %s: %w", e.cfg.SupportTo, err)
	}
	e.log.Info("support notified",
		slog.String("component", "notify"),
		slog.String("subject", subject))
	return nil
}
