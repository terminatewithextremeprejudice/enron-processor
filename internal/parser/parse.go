package parser

import (
	"bytes"
	"time"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// parseMail turns one raw email into a ParsedMessage. It is pure (no I/O) and
// returns an error when the bytes do not describe a usable Enron mail — that
// error is exactly what the DLQ carries. A "usable" mail has at least an
// addressable From: (the schema's message.sender_id is NOT NULL, so a sender
// is mandatory) and parses without net/mail reporting a hard error.
func parseMail(env *enronv1.RawEnvelope) (*enronv1.ParsedMessage, error) {
	if env == nil {
		return nil, errors.New("parser: nil envelope")
	}
	if len(env.RawBytes) == 0 {
		return nil, errors.New("parser: empty payload")
	}

	msg, err := mail.ReadMessage(bytes.NewReader(env.RawBytes))
	if err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	pm := &enronv1.ParsedMessage{
		IdempotencyKey: env.IdempotencyKey,
		SourcePath:     env.SourcePath,
		MessageId:      clean(msg.Header.Get("Message-ID")),
		Subject:        clean(msg.Header.Get("Subject")),
	}

	if addr, err := mail.ParseAddress(msg.Header.Get("From")); err == nil {
		pm.SenderName = clean(addr.Name)
		pm.SenderEmail = clean(addr.Address)
	} else {
		// Fall back to a bare address (Enron mails sometimes carry a
		// quoted display name net/mail chokes on); if that fails too there
		// is no From and the mail is unparseable by design.
		if bare := clean(msg.Header.Get("From")); bare != "" {
			pm.SenderEmail = bare
		}
	}
	if pm.SenderEmail == "" {
		return nil, errors.New("no parseable From: header (sender is required)")
	}

	pm.To = recipients(msg.Header["To"])
	pm.Cc = recipients(msg.Header["Cc"])
	pm.Bcc = recipients(msg.Header["Bcc"])
	pm.ReplyTo = stringsToAddresses(msg.Header["Reply-To"])

	if t, err := mail.ParseDate(msg.Header.Get("Date")); err == nil {
		pm.SentAtUnix = t.Unix()
	}

	pm.BodyText, pm.BodyHtml = splitBody(msg)

	return pm, nil
}

func cleanHeader(v string) string { return strings.TrimSpace(v) }

func stringsToAddresses(values []string) []string {
	addrs, err := mail.ParseAddressList(strings.Join(values, ", "))
	if err != nil {
		// Best-effort: fall back to the raw trimmed tokens.
		var out []string
		for _, v := range values {
			if t := clean(v); t != "" {
				out = append(out, t)
			}
		}
		return out
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a.Address != "" {
			out = append(out, clean(a.Address))
		}
	}
	return out
}

// addrs maps a folded RFC-822 address-list header (e.g. To/Cc/Bcc) to Recipients.
func addrs(values []string) []*enronv1.Recipient {
	var out []*enronv1.Recipient
	for _, raw := range values {
		list, err := mail.ParseAddressList(raw)
		if err != nil {
			// Fall back to a single best-effort address rather than drop the line.
			if a, e := mail.ParseAddress(raw); e == nil && a.Address != "" {
				out = append(out, &enronv1.Recipient{Name: a.Name, Email: a.Address})
			}
			continue
		}
		for _, a := range list {
			if a.Address == "" {
				continue
			}
			out = append(out, &enronv1.Recipient{Name: a.Name, Email: a.Address})
		}
	}
	return out
}

func replyTo(values []string) []string {
	var out []string
	for _, raw := range values {
		list, err := mail.ParseAddressList(raw)
		if err == nil {
			for _, a := range list { if a.Address != "" { out = append(out, a.Address) } }

			continue
		}
		if s := strings.TrimSpace(raw); s != "" { out = append(out, s) }
	}
	return out
}

// lastReceived finds the most recent Received header timestamp (mail carries
// one per hop; the last is the closest to the recipient, the natural ingest
// time). Returns the parsed time plus whether a usable timestamp existed.
func lastReceived(values []string) (time.Time, bool) {
	var best time.Time
	found := false
	for _, raw := range values {
		// A Received header may embed the date after the "from ..." / "by ..."
		// parts; the date is whatever the RFC-2822 date parser can find. Extract
		// the bracketed/localtime portion.
		if t, err := parseReceivedDate(raw); err == nil {
			if !found || t.After(best) { best, found = t, true }
		}
	}
	return best, found
}

func parseReceivedDate(raw string) (time.Time, error) {
	cands := []string{strings.TrimSpace(raw)}
	if i := strings.LastIndex(raw, ";"); i >= 0 {
		cands = append(cands, strings.TrimSpace(raw[i+1:]))
	}
	for _, c := range cands {
		if t, err := mail.ParseDate(c); err == nil { return t, nil }
	}
	return time.Time{}, fmt.Errorf("unparseable")
}

// splitBody separates the mail body into its text/plain and text/html parts.
// For a multipart message it walks each part and keeps the first text and
// first html subpart (the common "text then html" Enron layout, but resilient
// to order). A non-multipart body is typed by its own Content-Type.
func splitBody(msg *mail.Message) (text, html string) {
	ct := msg.Header.Get("Content-Type")
	params := make(map[string]string)
	mediaType := ct
	if mt, p, err := mime.ParseMediaType(ct); err == nil {
		mediaType = mt
		params = p
	}

	boundary := params["boundary"]
	if boundary != "" {
		mp := multipart.NewReader(msg.Body, boundary)
		for {
			part, err := mp.NextPart()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				continue // skip an unreadable part rather than fail the whole mail
			}
			data, rerr := io.ReadAll(part)
			if rerr != nil {
				continue
			}
			pct := strings.ToLower(part.Header.Get("Content-Type"))
			switch {
			case strings.Contains(pct, "text/html"):
				if html == "" {
					html = string(data)
				}
			case strings.Contains(pct, "text/plain"):
				if text == "" {
					text = string(data)
				}
			}
			_ = part.Close()
		}
		return text, html
	}

	data, _ := io.ReadAll(msg.Body)
	body := string(data)
	if strings.Contains(strings.ToLower(mediaType), "text/html") {
		return "", body
	}
	return body, ""
}

// clean trims surrounding whitespace from a header value (the proto fields are
// the normalized form; a raw CRLF/LF inside is already stripped by net/mail).
func clean(s string) string { return strings.TrimSpace(s) }

// recipients parses a RFC-5322 address-list header (To/Cc/Bcc) into the
// Recipient protos, preserving name + email.
func recipients(values []string) []*enronv1.Recipient {
	if len(values) == 0 {
		return nil
	}
	joined := strings.Join(values, ", ") // net/mail already splits on commas; join back for ParseAddressList
	addrs, err := mail.ParseAddressList(joined)
	if err != nil {
		return nil
	}
	out := make([]*enronv1.Recipient, 0, len(addrs))
	for _, a := range addrs {
		if a.Address == "" {
			continue
		}
		out = append(out, &enronv1.Recipient{Name: clean(a.Name), Email: clean(a.Address)})
	}
	return out
}
