package producer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestE2EIngestWalk drives the real service surface against the real
// dataset: Start → dataset download + extraction → full maildir walk →
// Status, over actual gRPC. It is deliberately gated: it downloads a
// ~290 MiB tarball and walks ~500k files.
//
//	ENRON_E2E=1 go test -count=1 -timeout 2h -run TestE2EIngestWalk ./internal/producer/
//
// ENRON_E2E_DATA_DIR points DataDir at a dir that already contains
// enron_mail_20150507/ to skip the download.
// ENRON_E2E_MIN_TOTAL sanity lower bound on discovered files (default 100k).
func TestE2EIngestWalk(t *testing.T) {
	if os.Getenv("ENRON_E2E") == "" {
		t.Skip("ENRON_E2E not set; this test downloads the full dataset (~290 MiB) and walks ~500k files")
	}

	dataDir := os.Getenv("ENRON_E2E_DATA_DIR")
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := Config{
		ListenAddr:  "127.0.0.1:39217",
		DataDir:     dataDir,
		DatasetName: datasetNameCanonical,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New(cfg, log)
	errc := make(chan error, 1)
	go func() { errc <- srv.Run(ctx) }()
	defer func() {
		srv.Close()
		if runErr := <-errc; runErr != nil {
			t.Logf("server exited: %v", runErr)
		}
	}()

	conn, err := grpc.NewClient("127.0.0.1:39217", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := enronv1.NewProducerClient(conn)

	// Wait for readiness with a bounded Status round-trip per attempt.
	var ready bool
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
		if _, err := client.Status(cctx, &enronv1.StatusRequest{}); err == nil {
			ready = true
		}
		ccancel()
		if ready {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !ready {
		t.Fatal("producer never became ready")
	}

	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	reply, err := client.Start(sctx, &enronv1.StartRequest{TriggeredBy: "e2e-test"})
	scancel()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !reply.GetStarted() {
		t.Fatalf("Start returned started=false in state %v", reply.GetState())
	}
	if reply.GetState() != enronv1.ProducerState_PRODUCER_STATE_RUNNING {
		t.Fatalf("state after Start = %v, want RUNNING", reply.GetState())
	}

	// Poll until FINISHED. Download (~700 KiB/s from CMU → roughly 8 min)
	// plus the walk; allow 25 min by default, overridable.
	timeout := 25 * time.Minute
	if v := os.Getenv("ENRON_E2E_TIMEOUT_MIN"); v != "" {
		if n, perr := parseInt(v); perr == nil && n > 0 {
			timeout = time.Duration(n) * time.Minute
		}
	}
	minTotal := int64(100000)
	if v := os.Getenv("ENRON_E2E_MIN_TOTAL"); v != "" {
		if n, perr := parseInt(v); perr == nil && n > 0 {
			minTotal = int64(n)
		}
	}

	var last *enronv1.StatusReply
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		time.Sleep(5 * time.Second)
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		st, perr := client.Status(pctx, &enronv1.StatusRequest{})
		pcancel()
		if perr != nil {
			t.Logf("status poll failed: %v", perr)
			continue
		}
		last = st
		t.Logf("state=%s total=%d emitted=%d", st.GetState(), st.GetTotalMails(), st.GetEmitted())
		switch st.GetState() {
		case enronv1.ProducerState_PRODUCER_STATE_FINISHED:
			if st.GetTotalMails() < minTotal {
				t.Fatalf("walk finished with only %d files, want >= %d", st.GetTotalMails(), minTotal)
			}
			if st.GetEmitted() != st.GetTotalMails() {
				t.Fatalf("emitted=%d != total=%d (stub sink must not drop envelopes)", st.GetEmitted(), st.GetTotalMails())
			}
			if st.GetWalkFinishedAtMs() <= st.GetWalkStartedAtMs() {
				t.Fatalf("finishedAt %d not after startedAt %d", st.GetWalkFinishedAtMs(), st.GetWalkStartedAtMs())
			}
			t.Logf("e2e OK: %d files walked and emitted", st.GetTotalMails())
			return
		case enronv1.ProducerState_PRODUCER_STATE_IDLE:
			t.Fatalf("walk failed during e2e (see producer logs); last counters: %+v", st)
		}
	}
	t.Fatalf("timed out after %v; last status: %+v", timeout, last)
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
