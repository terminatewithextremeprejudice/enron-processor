package producer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// WalkStats is the outcome of one walk, mirrored onto StatusReply
// (total_mails / emitted).
type WalkStats struct {
	Total   int64 // mail files discovered (invariant: Total == Emitted + Failed)
	Emitted int64 // envelopes the emit sink accepted
	Failed  int64 // files that could not be read or emitted
}

// walkProgressEvery throttles the in-walk progress line — the full dataset
// is on the order of 500k files, one log line per file would swamp it.
const walkProgressEvery = 25000

// idempotencyKey is the pipeline-wide dedupe id (envelope.proto + README):
// hex sha256 of the dataset-relative, slash-separated mail path. It is
// deterministic across producer restarts, re-runs and DLQ replays, so the
// same physical mail always maps to the same key no matter who or how many
// times it is walked.
func idempotencyKey(relSlashPath string) string {
	sum := sha256.Sum256([]byte(relSlashPath))
	return hex.EncodeToString(sum[:])
}

// buildEnvelope assembles the Kafka transport message for one mail file.
// Raw bytes are capped at cfg.MaxEnvelopeBytes if set; excess is truncated.
// This allows the producer to be a ferry while handling unusually large files.
func buildEnvelope(cfg Config, relSlashPath string, raw []byte, pid string, producedAt time.Time) *enronv1.RawEnvelope {
	if cfg.MaxEnvelopeBytes > 0 && int64(len(raw)) > cfg.MaxEnvelopeBytes {
		raw = raw[:int(cfg.MaxEnvelopeBytes)]
	}
	return &enronv1.RawEnvelope{
		IdempotencyKey: idempotencyKey(relSlashPath),
		SourcePath:     relSlashPath,
		RawBytes:       raw,
		ProducerId:     pid,
		ProducedAtMs:   producedAt.UnixMilli(),
	}
}

// Walk visits every regular file under root and hands exactly one envelope
// to emit per file. The producer is deliberately dumb — no parsing, no
// filtering (mail, attached binaries, .eml blobs alike; whatever is
// unparseable is the parser's job to route to the DLQ), no rewriting: the
// bytes that go out are the bytes on disk.
//
// Non-regular entries (symlinks, devices) are skipped and symlinked
// directories are not followed, so the same physical mail can never be
// emitted twice through two names. Per-file failures (unreadable file,
// emit rejection) are counted, logged with the dataset-relative path, and
// walked around — one bad file must not sink the rest of the tree. A walk
// returns partial stats plus an error only when cancelled or when the
// walker itself dies.
func Walk(ctx context.Context, cfg Config, root string, emit EmitFunc, pid string, log *slog.Logger) (WalkStats, error) {
	var total, emitted, failed atomic.Int64

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, entryErr error) error {
		if entryErr != nil {
			if d != nil {
				failed.Add(1)
				log.Error("walk entry unreadable", "path", path, "err", entryErr)
			}
			return nil // keep the rest of the tree alive
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err // shut down cleanly on cancellation
		}

		// A discovered file is a discovered file: counted up front so
		// progress reflects discovery even while one file is still being
		// read or emitted.
		total.Add(1)

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			failed.Add(1)
			log.Error("relativize path", "path", path, "err", relErr)
			return nil
		}
		relSlash := filepath.ToSlash(rel)

			// Open the file and limit read to config's MaxEnvelopeBytes
			f, err := os.Open(path)
			if err != nil {
				failed.Add(1)
				log.Error("read mail file", "path", relSlash, "err", err)
				return nil
			}
			// Use LimitReader to cap the read at MaxEnvelopeBytes
			var limited io.Reader = io.LimitReader(f, cfg.MaxEnvelopeBytes)
			// Read at least header bytes if file is smaller, or exactly the limit
			raw, readErr := io.ReadAll(limited)
			f.Close()
			if readErr != nil {
				failed.Add(1)
				log.Error("read mail file", "path", relSlash, "err", readErr)
				return nil
			}

			// Skip VCALENDAR format - these are calendar data files, not emails.
			// Filter them early so they're not counted in total/emitted stats.
			if bytes.Contains(bytes.ToUpper(raw), []byte("BEGIN:VCALENDAR")) {
				log.Debug("skipping VCALENDAR file", "path", relSlash)
				return nil
			}

			total.Add(1)

					if err := emit(ctx, buildEnvelope(cfg, relSlash, raw, pid, time.Now())); err != nil {
					failed.Add(1)
					log.Error("emit envelope", "path", relSlash, "err", err)
					return nil
				}
				// Only increment emitted after emit() succeeds (returns nil)
				emitted.Add(1)

		if n := total.Load(); n%walkProgressEvery == 0 {
			log.Info("walk progress",
				"files", n,
				"emitted", emitted.Load(),
				"failed", failed.Load())
		}
		return nil
	})

	stats := WalkStats{Total: total.Load(), Emitted: emitted.Load(), Failed: failed.Load()}
	if walkErr != nil {
		return stats, fmt.Errorf("walk %s: %w", root, walkErr)
	}
	return stats, nil
}
