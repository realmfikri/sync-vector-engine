package syncstate

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/types"
)

var (
	// ErrCausalityGap is returned when an operation is queued because the server
	// has not yet observed one of its causal predecessors.
	ErrCausalityGap = errors.New("operation delayed: causal gap detected")
)

// OperationApplier is invoked when an operation is ready to be executed.
type OperationApplier func(types.Operation) error

// OperationReorderBuffer holds operations that cannot be applied yet because
// the local vector clock lags behind the incoming operation.
type OperationReorderBuffer struct {
	mu       sync.Mutex
	tracker  *VectorClockTracker
	pending  map[types.DocumentID][]types.Operation
	logger   zerolog.Logger
	reorders *prometheus.CounterVec
}

// NewOperationReorderBuffer constructs a buffer with the provided clock
// tracker and logger.
func NewOperationReorderBuffer(tracker *VectorClockTracker, logger zerolog.Logger) *OperationReorderBuffer {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sync",
		Subsystem: "vector_clock",
		Name:      "operations_reordered_total",
		Help:      "Number of operations applied after waiting for causal predecessors.",
	}, []string{"document_id"})

	if err := prometheus.Register(counter); err != nil {
		if regErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = regErr.ExistingCollector.(*prometheus.CounterVec)
		}
	}

	return &OperationReorderBuffer{
		tracker:  tracker,
		logger:   logger,
		pending:  make(map[types.DocumentID][]types.Operation),
		reorders: counter,
	}
}

// HandleOperation determines whether the provided operation can be applied
// immediately. If the local clock does not dominate the incoming clock, the
// operation is queued until its dependencies arrive.
func (b *OperationReorderBuffer) HandleOperation(op types.Operation, apply OperationApplier) error {
	if op.VectorClock == nil {
		op.VectorClock = make(types.VectorClock)
	}

	if !b.tracker.Dominates(op.Document, op.VectorClock) {
		b.enqueue(op)
		b.logger.Info().
			Str("document", string(op.Document)).
			Str("operation", string(op.ID)).
			Str("client", string(op.Client)).
			Msg("queued operation pending causal predecessors")
		return ErrCausalityGap
	}

	if err := apply(op); err != nil {
		return err
	}
	b.tracker.MergeRemote(op.Document, op.VectorClock)

	return b.drain(op.Document, apply)
}

// drain re-checks pending operations to see if any are now unblocked.
func (b *OperationReorderBuffer) drain(docID types.DocumentID, apply OperationApplier) error {
	for {
		op, ok := b.dequeueReady(docID)
		if !ok {
			return nil
		}

		b.logger.Info().
			Str("document", string(docID)).
			Str("operation", string(op.ID)).
			Str("client", string(op.Client)).
			Msg("applying previously queued operation")
		b.reorders.WithLabelValues(string(docID)).Inc()

		if err := apply(op); err != nil {
			return err
		}
		b.tracker.MergeRemote(docID, op.VectorClock)
	}
}

func (b *OperationReorderBuffer) enqueue(op types.Operation) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pending[op.Document] = append(b.pending[op.Document], op)
}

func (b *OperationReorderBuffer) dequeueReady(docID types.DocumentID) (types.Operation, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue := b.pending[docID]
	if len(queue) == 0 {
		return types.Operation{}, false
	}

	clock := b.tracker.Snapshot(docID)
	for i, op := range queue {
		if clock.Dominates(op.VectorClock) {
			// remove from slice while preserving order for the rest
			b.pending[docID] = append(queue[:i], queue[i+1:]...)
			return op, true
		}
	}

	return types.Operation{}, false
}
