package producer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestIdempotencyKeyIsDeterministicSHA256(t *testing.T) {
	const path = "1/all/100"
	key := idempotencyKey(path)
	sum := sha256.Sum256([]byte(path))
	want := hex.EncodeToString(sum[:])
	if key != want {
		t.Fatalf("idempotencyKey(%q) = %s, want sha256 hex %s", path, key, want)
	}
	if idempotencyKey(path) != key {
		t.Fatal("idempotency key is not deterministic")
	}
	if idempotencyKey("2/all/100") == key {
		t.Fatal("different paths produced the same key")
	}
}

func TestBuildEnvelopeCarriesAllFields(t *testing.T) {
	raw := []byte("From: a@b\nSubject: hi\n\nbody")
	env := buildEnvelope("1/all/100", raw, "host/42", time.Now())

	if env.IdempotencyKey == "" {
		t.Fatal("missing idempotency key")
	}
	if env.SourcePath != "1/all/100" {
		t.Fatalf("SourcePath = %q", env.SourcePath)
	}
	if !bytes.Equal(env.RawBytes, raw) {
		t.Fatal("RawBytes was modified")
	}
	if env.ProducerId != "host/42" {
		t.Fatalf("ProducerId = %q", env.ProducerId)
	}
	if env.ProducedAtMs <= 0 {
		t.Fatalf("ProducedAtMs = %d", env.ProducedAtMs)
	}
}

func TestWalkEmitsOneEnvelopePerRegularFile(t *testing.T) {
	root := t.TempDir()
	writeMail := func(rel, body string) {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeMail("1/all/1", "From: a@b\nSubject: x\n\nbody one")
	writeMail("1/inbox/7", "From: c@d\n\nbody two")
	writeMail("1/attachments/blob.bin", "not mail at all")
	// Symlink into the user dir: must be skipped, not double-emitted, and
	// must not make the walk loop or fail.
	if err := os.Symlink("all/1", filepath.Join(root, "1", "dup")); err != nil {
		t.Fatal(err)
	}

	type got struct {
		key string
		raw []byte
	}
	var mu sync.Mutex
	var emitted []got
	emit := func(_ context.Context, env *enronv1.RawEnvelope) error {
		mu.Lock()
		emitted = append(emitted, got{env.IdempotencyKey, env.RawBytes})
		mu.Unlock()
		return nil
	}

	stats, err := Walk(context.Background(), root, emit, "test/1", quietLog())
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	wantTotal := int64(3)
	if stats.Total != wantTotal {
		t.Fatalf("Total = %d, want %d (symlink must not count)", stats.Total, wantTotal)
	}
	if stats.Emitted != wantTotal {
		t.Fatalf("Emitted = %d, want %d", stats.Emitted, wantTotal)
	}
	if stats.Failed != 0 {
		t.Fatalf("Failed = %d, want 0", stats.Failed)
	}
	if len(emitted) != int(wantTotal) {
		t.Fatalf("envelopes handed to sink = %d, want %d", len(emitted), wantTotal)
	}

	// Every envelope must be a faithful ferry: untouched bytes and a key.
	seen := map[string]bool{}
	for _, e := range emitted {
		seen[string(e.raw)] = true
		if e.key == "" {
			t.Fatal("envelope without idempotency key")
		}
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct envelopes, got %d", len(seen))
	}
}

func TestWalkCountsEmitFailuresAndKeepsGoing(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"1/all/1", "1/all/2", "1/all/3"} {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("mail"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	emit := func(_ context.Context, env *enronv1.RawEnvelope) error {
		if env.SourcePath == "1/all/2" {
			return context.DeadlineExceeded // simulate one rejection
		}
		return nil
	}
	wantFailed := int64(1)
	stats, err := Walk(context.Background(), root, emit, "test/1", quietLog())
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if stats.Total != 3 || stats.Emitted != 2 || stats.Failed != wantFailed {
		t.Fatalf("stats = %+v, want Total=3 Emitted=2 Failed=1", stats)
	}
}

func TestWalkAbortsOnCancellation(t *testing.T) {
	root := t.TempDir()
	full := filepath.Join(root, "1/all/1")
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("mail"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	if _, err := Walk(ctx, root, func(context.Context, *enronv1.RawEnvelope) error { return nil }, "test/1", quietLog()); err == nil {
		t.Fatal("Walk should fail on a cancelled context")
	}
}
