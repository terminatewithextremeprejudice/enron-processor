package producer

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/grpc"
)

// Server implements enron.v1.Producer (Start/Status) over gRPC — exactly
// the surface the api service consumes.
//
// Ingestion revision: Start kicks off the real dataset pipeline — ensure
// the tree is present (download + extract on first runs), walk it, emit
// one RawEnvelope per mail file. Envelopes are carried to cfg.RawTopic over
// the configured Sink (emit.go): a sarama sync producer for the production
// leg, or a dummy acceptance sink when config.Sink is "dummy".
type Server struct {
	enronv1.UnimplementedProducerServer
	cfg Config
	log *slog.Logger

	// sink is the envelope destination (sarama or dummy); built by New from
	// cfg.Sink and closed by Close.
	sink Sink

	// Walk lifecycle. Token pattern: every Start bumps walkToken, and a
	// walk may only apply its final state while it is still the newest
	// walk. That keeps a slow (or superseded) walk from stomping a newer
	// walk's state — the same "last writer wins by generation" guarantee a
	// real walk would need.
	mu         sync.RWMutex
	state      enronv1.ProducerState
	walkToken  uint64
	startedAt  int64 // unix ms; 0 before the first walk
	finishedAt int64 // unix ms; 0 while a walk is in flight

	// Per-walk counters surfaced through Status. Reset by each fresh Start
	// and filled in by finishWalk, so a finished walk's numbers never
	// bleed into the next one.
	totalMails int64 // mail files discovered
	emitted    int64 // envelopes accepted by the emit sink
	dlq        int64 // always 0 in this revision: no DLQ routing yet

	// Shutdown plumbing (set by Run).
	ctx      context.Context
	cancel   context.CancelFunc
	srv      *grpc.Server
	closing  bool // guards sink.Close in Close so it runs exactly once
}

// New wires the control surface and the emit sink. It builds the sink from
// cfg.Sink: a sarama sync producer for the kafka leg (which dials the broker
// and can fail) or a dummy sink (which never does). The state starts at
// UNSPECIFIED ("no walk has been requested yet"), which is startable.
func New(cfg Config, log *slog.Logger) (*Server, error) {
	sink, err := newSink(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("build emit sink: %w", err)
	}
	return &Server{
		cfg:   cfg,
		log:   log,
		sink:  sink,
		state: enronv1.ProducerState_PRODUCER_STATE_UNSPECIFIED,
	}, nil
}

// Start implements enron.v1.Producer.Start.
func (s *Server) Start(ctx context.Context, req *enronv1.StartRequest) (*enronv1.StartReply, error) {
	s.mu.Lock()
	started := false
	switch s.state {
	case enronv1.ProducerState_PRODUCER_STATE_UNSPECIFIED,
		enronv1.ProducerState_PRODUCER_STATE_IDLE,
		enronv1.ProducerState_PRODUCER_STATE_FINISHED:
		s.walkToken++
		s.state = enronv1.ProducerState_PRODUCER_STATE_RUNNING
		s.startedAt = time.Now().UnixMilli()
		s.finishedAt = 0
		// A fresh walk starts fresh counters; the previous walk's numbers
		// must not contaminate its Status.
		s.totalMails = 0
		s.emitted = 0
		s.dlq = 0
		started = true
		// case PRODUCER_STATE_RUNNING: no-op (already walking)
	}
	token, state := s.walkToken, s.state
	s.mu.Unlock()

	s.log.Info("start requested",
		"started", started,
		"state", state.String(),
		"triggered_by", req.GetTriggeredBy())

	if started {
		go s.runWalk(token)
	}
	return &enronv1.StartReply{Started: started, State: state}, nil
}

// Status implements enron.v1.Producer.Status.
func (s *Server) Status(ctx context.Context, req *enronv1.StatusRequest) (*enronv1.StatusReply, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &enronv1.StatusReply{
		State:            s.state,
		TotalMails:       s.totalMails,
		Emitted:          s.emitted,
		Dlq:              s.dlq,
		WalkStartedAtMs:  s.startedAt,
		WalkFinishedAtMs: s.finishedAt,
	}, nil
}

// runWalk drives one real ingestion pass: ensure the dataset tree exists
// (download + extract on a first run), walk it, and hand one envelope per
// mail file to the (currently stubbed) emit sink. The state transitions
// around it are unchanged from the stub revision — RUNNING in flight,
// FINISHED on a clean walk, IDLE on failure so the next Start is the
// retry.
func (s *Server) runWalk(token uint64) {
	root, err := ensureDataset(s.ctx, s.cfg, s.log)
	if err != nil {
		s.log.Error("dataset unavailable, walk not started", "err", err)
		s.finishWalk(token, WalkStats{}, fmt.Errorf("ensure dataset: %w", err))
		return
	}

	stats, err := Walk(s.ctx, root, s.emit, producerID(), s.log)
	if err != nil {
		s.log.Error("walk aborted",
			"err", err,
			"files", stats.Total,
			"emitted", stats.Emitted,
			"failed", stats.Failed)
	}
	s.finishWalk(token, stats, err)
}

// finishWalk records the walk's outcome — and only then updates state — if
// (and only if) this walk is still the newest one.
func (s *Server) finishWalk(token uint64, stats WalkStats, walkErr error) {
	s.mu.Lock()
	current := s.walkToken == token
	if current {
		s.totalMails = stats.Total
		s.emitted = stats.Emitted
		if walkErr == nil {
			s.state = enronv1.ProducerState_PRODUCER_STATE_FINISHED
			s.finishedAt = time.Now().UnixMilli()
		} else {
			// A failed walk is not finished: IDLE is exactly "no walk in
			// flight, a Start will begin one" — i.e. the retry posture.
			s.state = enronv1.ProducerState_PRODUCER_STATE_IDLE
			s.finishedAt = 0
		}
	}
	s.mu.Unlock()

	if !current {
		s.log.Warn("walk completion superseded by a newer walk", "walk_token", token)
		return
	}
	switch {
	case walkErr == nil:
		s.log.Info("walk finished",
			"files", stats.Total,
			"emitted", stats.Emitted,
			"failed", stats.Failed)
	default:
		s.log.Error("walk failed; call Start to retry", "err", walkErr)
	}
}

// Run serves gRPC until ctx is cancelled, then drains in-flight RPCs with
// GracefulStop (hard Stop after a deadline, since GracefulStop alone can
// wedge on a client that never finishes).
func (s *Server) Run(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		s.cancel()
		return fmt.Errorf("listen %q: %w", s.cfg.ListenAddr, err)
	}

	s.srv = grpc.NewServer()
	enronv1.RegisterProducerServer(s.srv, s)

	errc := make(chan error, 1)
	go func() { errc <- s.srv.Serve(ln) }()

	s.log.Info("producer ready",
		"addr", ln.Addr().String(),
		"data_dir", s.cfg.DataDir,
		"dataset", s.cfg.DatasetName)

	select {
	case <-s.ctx.Done():
		stopped := make(chan struct{})
		go func() {
			s.srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			s.log.Warn("graceful stop timed out; stopping now")
			s.srv.Stop()
		}
		return nil
	case err := <-errc:
		s.cancel()
		return err // nil means the server was stopped out-of-band
	}
}

// Close cancels the service context (bounds any in-flight walk).
func (s *Server) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}
