package parser

import (
	"strings"
	"testing"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

func mustEnv(raw string) *enronv1.RawEnvelope {
	return &enronv1.RawEnvelope{IdempotencyKey: "key-1", SourcePath: "user/x.msg", RawBytes: []byte(raw)}
}

func TestParseSimpleMail(t *testing.T) {
	const raw = "From: Alice <alice@example.com>\r\n" +
		"To: Bob <bob@example.com>, carol@example.com\r\n" +
		"Cc: dave@example.com\r\n" +
		"Reply-To: alice@example.com\r\n" +
		"Subject: Hello there\r\n" +
		"Date: Mon, 5 Nov 2026 08:00:01 +0200\r\n" +
		"Message-ID: <1234@example.com>\r\n" +
		"\r\n" +
		"Line one\r\n" +
		"Line two\r\n"

	pm, err := parseMail(mustEnv(raw))
	if err != nil {
		t.Fatalf("parseMail: %v", err)
	}
	if pm.SenderEmail != "alice@example.com" { t.Errorf("sender email = %q", pm.SenderEmail) }
	if len(pm.To) != 2 { t.Errorf("to len = %d, want 2: %+v", len(pm.To), pm.To) }
	if len(pm.Cc) != 1 || pm.Cc[0].Email != "dave@example.com" { t.Errorf("cc = %+v",
 pm.Cc) }
	if pm.Subject != "Hello there" { t.Errorf("subject = %q", pm.Subject) }
	if pm.MessageId != "<1234@example.com>" { t.Errorf("msg id = %q", pm.MessageId) }

	if pm.SentAtUnix <= 0 { t.Errorf("sentAt = %d", pm.SentAtUnix) }
	if !strings.Contains(pm.BodyText, "Line one") { t.Errorf("body = %q", pm.BodyText) }
	if pm.BodyHtml != "" { t.Errorf("html = %q, want empty", pm.BodyHtml) }
	if pm.IdempotencyKey != "key-1" { t.Errorf("idem key = %q", pm.IdempotencyKey) }

}

func TestParseMultipartHTML(t *testing.T) {
	const raw = "From: A <a@x.com>\r\n" +
		"Subject: hi\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/alternative; boundary=\"BOUND\"\r\n" +
		"\r\n" +
		"--BOUND\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"plain body\r\n" +
		"--BOUND\r\n" +
		"Content-Type: text/html\r\n" +
		"\r\n" +
		"<b>html body</b>\r\n" +
		"--BOUND--\r\n"

	pm, err := parseMail(mustEnv(raw))
	if err != nil { t.Fatalf("parse: %v", err) }
	if pm.BodyText != "plain body" { t.Errorf("text = %q", pm.BodyText) }
	if !strings.Contains(pm.BodyHtml, "<b>html body</b>") { t.Errorf("html = %q", pm.BodyHtml) }
}

func TestParseNoSenderFails(t *testing.T) {
	const raw = "Subject: no from\r\n\r\nbody\r\n"
	if _, err := parseMail(mustEnv(raw)); err == nil {
		t.Fatal("expected error for mail with no sender, got success")
	}
}

func TestParseEmptyFails(t *testing.T) {
	if _, err := parseMail(mustEnv("")); err == nil {
		t.Fatal("expected error for empty mail")
	}
}
