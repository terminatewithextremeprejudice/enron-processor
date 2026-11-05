package api

import (
	"context"
	"fmt"
	"time"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Producer is the api's doorbell on the producer service (enron.v1.Producer).
// The method set is the whole contract the api needs; keeping it narrow
// pins the api to exactly the RPCs it uses (plus the startup probe).
type Producer interface {
	// Start requests (re)ingestion of the dataset. Idempotent by contract:
	// RUNNING → no-op (started=false); IDLE/FINISHED → a walk is (re)started.
	Start(ctx context.Context, triggeredBy string) (started bool, state enronv1.ProducerState, err error)

	// Status returns the producer's state and walk-progress counters.
	Status(ctx context.Context) (*enronv1.StatusReply, error)

	// Probe performs a bounded Status round-trip for startup diagnostics.
	Probe(ctx context.Context) error

	Close() error
}

// rpcDeadline bounds a single control-plane call. Both RPCs must answer
// fast — Start just flips state, Status just reads counters.
const rpcDeadline = 5 * time.Second

type producerGRPC struct {
	cc     *grpc.ClientConn
	client enronv1.ProducerClient
}

var _ Producer = (*producerGRPC)(nil)

// NewProducer creates a lazy gRPC connection to addr (host:port).
//
// Lazy on purpose: grpc.NewClient does not connect at all — the first RPC
// does. That lets the api service boot before the producer does (the common
// case while the pipeline is rolled out one binary at a time); until the
// producer is up, /start and /status fail with a clear 503 instead of the
// process refusing to start.
func NewProducer(addr string) (Producer, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create producer channel %q: %w", addr, err)
	}
	return &producerGRPC{cc: cc, client: enronv1.NewProducerClient(cc)}, nil
}

// Probe performs a bounded Status round-trip. Used at startup for a clear
// log line; failures are expected and not fatal.
func (p *producerGRPC) Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, rpcDeadline)
	defer cancel()
	_, err := p.client.Status(ctx, &enronv1.StatusRequest{})
	if err != nil {
		return fmt.Errorf("producer %q: %w", p.cc.Target(), err)
	}
	return nil
}

func (p *producerGRPC) Start(ctx context.Context, triggeredBy string) (bool, enronv1.ProducerState, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcDeadline)
	defer cancel()
	reply, err := p.client.Start(ctx, &enronv1.StartRequest{TriggeredBy: triggeredBy})
	if err != nil {
		return false, enronv1.ProducerState_PRODUCER_STATE_UNSPECIFIED, err
	}
	return reply.GetStarted(), reply.GetState(), nil
}

func (p *producerGRPC) Status(ctx context.Context) (*enronv1.StatusReply, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcDeadline)
	defer cancel()
	return p.client.Status(ctx, &enronv1.StatusRequest{})
}

func (p *producerGRPC) Close() error { return p.cc.Close() }
