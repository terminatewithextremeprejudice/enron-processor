package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// Response shapes for the three endpoints (plus /healthz). Field names are
// the external contract — see README "Required HTTP endpoints".
type startResponse struct {
	// started reports whether THIS call kicked off a walk; false means one
	// was already in flight and the call was a no-op.
	Started bool   `json:"started"`
	State   string `json:"state"` // ProducerState name, e.g. "PRODUCER_STATE_RUNNING"
}

type statusResponse struct {
	// Finished is the producer's verdict: its walk of the dataset is done.
	// (Upstream parse/persist lag may still be draining into the sink.)
	Finished bool `json:"finished"`
	// TotalMessages is the count of messages durably parsed and written
	// (rows in the message table) — the pipeline's effectively-once sink.
	TotalMessages int64 `json:"total_messages"`
}

type topSendersResponse struct {
	Senders []Sender `json:"senders"`
}

// Server is the api service: stdlib net/http in front of two upstreams,
// the producer (control plane, gRPC) and Postgres (data plane, SQL).
type Server struct {
	cfg    Config
	store  Store
	prod   Producer
	log    *slog.Logger
	srv    *http.Server
}

// New wires the HTTP handlers. Method+path patterns let the Go 1.22 mux
// enforce the verb per endpoint for free (405 + correct Allow header).
func New(cfg Config, store Store, prod Producer, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, store: store, prod: prod, log: log, srv: &http.Server{
		Addr:              cfg.ListenAddr,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", s.handleStart)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /top-senders", s.handleTopSenders)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.srv.Handler = s.observe(mux)
	return s
}

// Handler exposes the request router (used by tests).
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// Run serves until ctx is cancelled, then returns once Shutdown has
// drained in-flight requests.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %q: %w", s.cfg.ListenAddr, err)
	}
	errc := make(chan error, 1)
	go func() { errc <- s.srv.Serve(ln) }()

	s.log.Info("api listening", "addr", ln.Addr().String())
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			s.log.Warn("graceful shutdown incomplete", "err", err)
		}
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Close releases upstream connections (store + producer channel).
func (s *Server) Close() {
	if err := s.store.Close(); err != nil {
		s.log.Warn("close store", "err", err)
	}
	if err := s.prod.Close(); err != nil {
		s.log.Warn("close producer", "err", err)
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// handleStart: POST /start — trigger (re)ingestion.
//
// The api holds no state of its own: the producer is the actor, so it
// alone decides whether the call started a walk or was a no-op. Idempotent
// by contract — re-calling is always safe (downstream dedupes by the
// idempotency key, so a re-walk over an already-parsed dataset cannot
// double-count anything).
func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	triggeredBy := "api:POST /start from " + r.RemoteAddr

	started, state, err := s.prod.Start(ctx, triggeredBy)
	if err != nil {
		s.log.Error("start failed", "err", err, "by", triggeredBy)
		s.fault(w, "producer control service unavailable: "+err.Error())
		return
	}
	s.log.Info("ingestion start requested", "started", started, "state", state, "by", triggeredBy)
	s.json(w, http.StatusAccepted, startResponse{Started: started, State: state.String()})
}

// handleStatus: GET /status — finished? + total messages processed.
//
// Two sources of truth, both required: the producer says whether the walk
// is done, Postgres says how many messages it has made durable. Either one
// unreachable means the answer would be a guess → 503, with the reason.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	reply, err := s.prod.Status(ctx)
	if err != nil {
		s.log.Error("producer status failed", "err", err)
		s.fault(w, "producer status unavailable: "+err.Error())
		return
	}
	total, err := s.store.TotalMessages(ctx)
	if err != nil {
		s.log.Error("total messages failed", "err", err)
		s.fault(w, "message count unavailable: "+err.Error())
		return
	}
	s.json(w, http.StatusOK, statusResponse{
		Finished:      reply.GetState() == enronv1.ProducerState_PRODUCER_STATE_FINISHED,
		TotalMessages: total,
	})
}

// handleTopSenders: GET /top-senders — top N senders by message count.
func (s *Server) handleTopSenders(w http.ResponseWriter, r *http.Request) {
	senders, err := s.store.TopSenders(r.Context(), s.cfg.TopN)
	if err != nil {
		s.log.Error("top senders failed", "err", err)
		s.fault(w, "top senders unavailable: "+err.Error())
		return
	}
	if senders == nil {
		senders = []Sender{}
	}
	s.json(w, http.StatusOK, topSendersResponse{Senders: senders})
}

// handleHealthz: process liveness only. Deliberately does not check
// upstreams — a service that is up but whose peers are down should still
// answer liveness probes and explain the situation via /status and logs.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.json(w, http.StatusOK, map[string]bool{"ok": true})
}

// ---------------------------------------------------------------------------
// Plumbing
// ---------------------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (m *statusRecorder) WriteHeader(code int) {
	m.status = code
	m.ResponseWriter.WriteHeader(code)
}

// observe is the request logger (slog, one line per request).
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

func (s *Server) json(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		s.log.Error("encode response", "err", err) // headers already sent; log only
	}
}

func (s *Server) fault(w http.ResponseWriter, msg string) {
	s.json(w, http.StatusServiceUnavailable, map[string]string{"error": msg})
}
