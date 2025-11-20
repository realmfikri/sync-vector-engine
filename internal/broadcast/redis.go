package broadcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	proto "google.golang.org/protobuf/proto"

	operationsv1 "github.com/example/sync-vector-engine/internal/pb/proto"
	"github.com/example/sync-vector-engine/internal/types"
	"github.com/example/sync-vector-engine/internal/ws"
)

const (
	defaultTopicPrefix = "doc:"
	defaultDedupeTTL   = 2 * time.Minute
	maxBackoffDelay    = 30 * time.Second
)

type redisMessage struct {
	DocumentID  string `json:"document_id"`
	OperationID string `json:"operation_id"`
	ClientID    string `json:"client_id,omitempty"`
	Payload     []byte `json:"payload"`
	EnqueuedAt  int64  `json:"enqueued_at"`
}

// RedisBroadcaster publishes CRDT operation envelopes to Redis and fans
// them back out to local websocket clients across instances.
type RedisBroadcaster struct {
	client   *redis.Client
	registry *ws.ConnectionRegistry
	logger   zerolog.Logger

	topicPrefix string
	dedupeTTL   time.Duration

	seenMu sync.Mutex
	seen   map[string]time.Time

	latency *prometheus.HistogramVec
}

// NewRedisBroadcaster constructs a broadcaster backed by Redis Pub/Sub.
func NewRedisBroadcaster(client *redis.Client, registry *ws.ConnectionRegistry, logger zerolog.Logger) *RedisBroadcaster {
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "broadcast",
		Name:      "enqueue_to_send_seconds",
		Help:      "Observed latency between enqueue and delivery to websocket clients.",
		Buckets:   prometheus.LinearBuckets(0.005, 0.005, 12),
	}, []string{"document_id"})

	if err := prometheus.Register(histogram); err != nil {
		if regErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
			histogram = regErr.ExistingCollector.(*prometheus.HistogramVec)
		}
	}

	return &RedisBroadcaster{
		client:      client,
		registry:    registry,
		logger:      logger,
		topicPrefix: defaultTopicPrefix,
		dedupeTTL:   defaultDedupeTTL,
		seen:        make(map[string]time.Time),
		latency:     histogram,
	}
}

// Publish serializes an operation envelope and sends it to the document topic.
func (b *RedisBroadcaster) Publish(ctx context.Context, docID types.DocumentID, opID types.OperationID, clientID types.ClientID, env *operationsv1.OperationEnvelope) error {
	if b == nil || b.client == nil {
		return errors.New("nil broadcaster")
	}

	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	msg := redisMessage{
		DocumentID:  string(docID),
		OperationID: string(opID),
		ClientID:    string(clientID),
		Payload:     payload,
		EnqueuedAt:  time.Now().UTC().UnixNano(),
	}

	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode redis payload: %w", err)
	}

	topic := b.topic(docID)
	backoff := time.Second
	for {
		if err := b.client.Publish(ctx, topic, encoded).Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			b.logger.Warn().Err(err).Str("topic", topic).Dur("backoff", backoff).Msg("redis publish failed; retrying")
			select {
			case <-time.After(backoff):
				backoff = minDuration(backoff*2, maxBackoffDelay)
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// Start begins consuming redis pub/sub messages and dispatching them to
// websocket clients registered locally.
func (b *RedisBroadcaster) Start(ctx context.Context) {
	go b.run(ctx)
}

func (b *RedisBroadcaster) run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}

		pubsub := b.client.PSubscribe(ctx, fmt.Sprintf("%s*", b.topicPrefix))
		if err := b.consume(ctx, pubsub); err != nil && !errors.Is(err, context.Canceled) {
			b.logger.Warn().Err(err).Dur("backoff", backoff).Msg("redis subscription interrupted; retrying")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = minDuration(backoff*2, maxBackoffDelay)
		}
	}
}

func (b *RedisBroadcaster) consume(ctx context.Context, pubsub *redis.PubSub) error {
	defer pubsub.Close()

	ch := pubsub.Channel(redis.WithChannelSize(256))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return errors.New("pubsub channel closed")
			}
			if err := b.process(msg); err != nil {
				b.logger.Warn().Err(err).Msg("failed to process broadcast message")
			}
		}
	}
}

func (b *RedisBroadcaster) process(msg *redis.Message) error {
	var payload redisMessage
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	if payload.DocumentID == "" || payload.OperationID == "" {
		return errors.New("incomplete payload")
	}

	if b.isDuplicate(payload.DocumentID, payload.OperationID) {
		return nil
	}

	var latencySeconds float64
	if payload.EnqueuedAt > 0 {
		latencySeconds = float64(time.Since(time.Unix(0, payload.EnqueuedAt))) / float64(time.Second)
	}
	b.latency.WithLabelValues(payload.DocumentID).Observe(latencySeconds)

	b.registry.BroadcastBinaryByClientID(payload.DocumentID, payload.Payload, payload.ClientID)
	return nil
}

func (b *RedisBroadcaster) topic(docID types.DocumentID) string {
	return fmt.Sprintf("%s%s", b.topicPrefix, docID)
}

func (b *RedisBroadcaster) isDuplicate(docID, opID string) bool {
	key := docID + ":" + opID

	b.seenMu.Lock()
	defer b.seenMu.Unlock()

	if ts, ok := b.seen[key]; ok {
		if time.Since(ts) < b.dedupeTTL {
			return true
		}
	}

	b.seen[key] = time.Now()
	cutoff := time.Now().Add(-b.dedupeTTL)
	for k, ts := range b.seen {
		if ts.Before(cutoff) {
			delete(b.seen, k)
		}
	}
	return false
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
