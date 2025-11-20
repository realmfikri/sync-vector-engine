package presence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protojson"
	proto "google.golang.org/protobuf/proto"

	operationsv1 "github.com/example/sync-vector-engine/internal/pb/proto"
	"github.com/example/sync-vector-engine/internal/ws"
)

const (
	defaultTTL           = 45 * time.Second
	defaultChannelPrefix = "presence:doc:"
	scanBatchSize        = 100
)

// Service tracks presence heartbeats in Redis and relays updates to
// websocket clients.
type Service struct {
	client   *redis.Client
	registry *ws.ConnectionRegistry
	logger   zerolog.Logger

	ttl           time.Duration
	channelPrefix string

	mu     sync.RWMutex
	roster map[string]map[string]*operationsv1.PresenceUpdate
}

// NewService constructs a presence service backed by Redis.
func NewService(client *redis.Client, registry *ws.ConnectionRegistry, logger zerolog.Logger) *Service {
	return &Service{
		client:        client,
		registry:      registry,
		logger:        logger,
		ttl:           defaultTTL,
		channelPrefix: defaultChannelPrefix,
		roster:        make(map[string]map[string]*operationsv1.PresenceUpdate),
	}
}

// Start begins background maintenance goroutines.
func (s *Service) Start(ctx context.Context) {
	go s.subscribe(ctx)
	go s.expireLoop(ctx)
}

// HandlePing persists and broadcasts an updated presence record.
func (s *Service) HandlePing(ctx context.Context, conn *ws.Connection, update *operationsv1.PresenceUpdate) error {
	if update == nil {
		return errors.New("nil presence update")
	}
	if update.DocumentId == "" {
		update.DocumentId = conn.DocumentID()
	}
	if update.ClientId == "" {
		update.ClientId = conn.ClientID()
	}
	if update.DocumentId == "" || update.ClientId == "" {
		return errors.New("presence update missing identifiers")
	}
	if update.Metadata == nil {
		update.Metadata = conn.Metadata()
	}

	if err := s.persist(ctx, update); err != nil {
		return err
	}
	s.recordLocal(update)
	if err := s.publish(ctx, update); err != nil {
		s.logger.Warn().Err(err).Msg("failed to publish presence update")
	}

	s.broadcastLocal(update, conn)
	return nil
}

// Clear removes any cached presence for the document/client pair and notifies peers.
func (s *Service) Clear(ctx context.Context, documentID, clientID string) {
	if documentID == "" || clientID == "" {
		return
	}
	key := s.presenceKey(documentID, clientID)
	if err := s.client.Del(ctx, key).Err(); err != nil && !errors.Is(err, redis.Nil) {
		s.logger.Warn().Err(err).Str("key", key).Msg("failed to delete presence key")
	}

	removal := &operationsv1.PresenceUpdate{DocumentId: documentID, ClientId: clientID, Disconnected: true}
	s.recordLocal(removal)
	if err := s.publish(ctx, removal); err != nil {
		s.logger.Warn().Err(err).Msg("failed to publish presence removal")
	}
	s.broadcastLocal(removal, nil)
}

// SendRoster streams the current roster to a freshly connected client.
func (s *Service) SendRoster(ctx context.Context, conn *ws.Connection) error {
	updates, err := s.Roster(ctx, conn.DocumentID())
	if err != nil {
		return err
	}
	for _, update := range updates {
		env := s.envelope(update)
		if err := conn.SendEnvelope(env); err != nil {
			return fmt.Errorf("send roster entry: %w", err)
		}
	}
	return nil
}

// Roster loads the current presence roster for a given document.
func (s *Service) Roster(ctx context.Context, documentID string) ([]*operationsv1.PresenceUpdate, error) {
	iter := s.client.Scan(ctx, 0, s.presenceKey(documentID, "*"), scanBatchSize).Iterator()

	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan presence keys: %w", err)
	}

	if len(keys) == 0 {
		s.mu.Lock()
		delete(s.roster, documentID)
		s.mu.Unlock()
		return nil, nil
	}

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("fetch presence values: %w", err)
	}

	var updates []*operationsv1.PresenceUpdate
	for _, raw := range values {
		strVal, ok := raw.(string)
		if !ok || strVal == "" {
			continue
		}
		var update operationsv1.PresenceUpdate
		if err := protojson.Unmarshal([]byte(strVal), &update); err != nil {
			s.logger.Warn().Err(err).Msg("failed to decode presence value")
			continue
		}
		updates = append(updates, &update)
	}

	s.mu.Lock()
	roster := s.ensureRoster(documentID)
	for _, update := range updates {
		roster[update.ClientId] = proto.Clone(update).(*operationsv1.PresenceUpdate)
	}
	s.mu.Unlock()

	return updates, nil
}

func (s *Service) expireLoop(ctx context.Context) {
	ticker := time.NewTicker(s.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.pruneExpired(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) pruneExpired(ctx context.Context) {
	s.mu.RLock()
	snapshot := make(map[string][]string, len(s.roster))
	for doc, clients := range s.roster {
		ids := make([]string, 0, len(clients))
		for client := range clients {
			ids = append(ids, client)
		}
		snapshot[doc] = ids
	}
	s.mu.RUnlock()

	for doc, clients := range snapshot {
		for _, client := range clients {
			key := s.presenceKey(doc, client)
			exists, err := s.client.Exists(ctx, key).Result()
			if err != nil {
				s.logger.Warn().Err(err).Msg("failed to check presence ttl")
				continue
			}
			if exists == 0 {
				removal := &operationsv1.PresenceUpdate{DocumentId: doc, ClientId: client, Disconnected: true}
				s.logger.Debug().Str("document", doc).Str("client", client).Msg("presence expired")
				s.recordLocal(removal)
				if err := s.publish(ctx, removal); err != nil {
					s.logger.Warn().Err(err).Msg("failed to publish presence expiration")
				}
				s.broadcastLocal(removal, nil)
			}
		}
	}
}

func (s *Service) subscribe(ctx context.Context) {
	if s.client == nil {
		return
	}
	pubsub := s.client.PSubscribe(ctx, fmt.Sprintf("%s*", s.channelPrefix))
	defer pubsub.Close()

	ch := pubsub.Channel(redis.WithChannelSize(128))
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			update, err := s.decodeMessage(msg.Payload)
			if err != nil {
				s.logger.Warn().Err(err).Msg("failed to decode presence broadcast")
				continue
			}
			s.recordLocal(update)
			s.broadcastLocal(update, nil)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) broadcastLocal(update *operationsv1.PresenceUpdate, skip *ws.Connection) {
	env := s.envelope(update)
	s.registry.BroadcastEnvelope(update.DocumentId, env, skip)
}

func (s *Service) recordLocal(update *operationsv1.PresenceUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	roster := s.ensureRoster(update.DocumentId)
	if update.Disconnected {
		delete(roster, update.ClientId)
		if len(roster) == 0 {
			delete(s.roster, update.DocumentId)
		}
		return
	}
	roster[update.ClientId] = proto.Clone(update).(*operationsv1.PresenceUpdate)
}

func (s *Service) persist(ctx context.Context, update *operationsv1.PresenceUpdate) error {
	if s.client == nil {
		return errors.New("nil redis client")
	}
	key := s.presenceKey(update.DocumentId, update.ClientId)
	payload, err := protojson.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal presence: %w", err)
	}
	if err := s.client.Set(ctx, key, payload, s.ttl).Err(); err != nil {
		return fmt.Errorf("cache presence: %w", err)
	}
	return nil
}

func (s *Service) publish(ctx context.Context, update *operationsv1.PresenceUpdate) error {
	if s.client == nil {
		return errors.New("nil redis client")
	}
	payload, err := proto.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal presence update: %w", err)
	}
	return s.client.Publish(ctx, s.channel(update.DocumentId), payload).Err()
}

func (s *Service) envelope(update *operationsv1.PresenceUpdate) *operationsv1.OperationEnvelope {
	return &operationsv1.OperationEnvelope{
		StreamId:  update.DocumentId,
		Timestamp: time.Now().UTC().UnixNano(),
		Body: &operationsv1.OperationEnvelope_Presence{
			Presence: update,
		},
	}
}

func (s *Service) presenceKey(documentID, clientID string) string {
	return fmt.Sprintf("%s%s:client:%s", s.channelPrefix, documentID, clientID)
}

func (s *Service) channel(documentID string) string {
	return fmt.Sprintf("%s%s", s.channelPrefix, documentID)
}

func (s *Service) ensureRoster(documentID string) map[string]*operationsv1.PresenceUpdate {
	roster, ok := s.roster[documentID]
	if !ok {
		roster = make(map[string]*operationsv1.PresenceUpdate)
		s.roster[documentID] = roster
	}
	return roster
}

func (s *Service) decodeMessage(payload string) (*operationsv1.PresenceUpdate, error) {
	var update operationsv1.PresenceUpdate
	if err := proto.Unmarshal([]byte(payload), &update); err != nil {
		return nil, err
	}
	return &update, nil
}

// WrapHooks installs presence handlers into the provided hook set, preserving
// any existing callbacks for composition.
func (s *Service) WrapHooks(base ws.Hooks) ws.Hooks {
	basePresence := base.OnPresence
	base.OnPresence = func(ctx context.Context, conn *ws.Connection, update *operationsv1.PresenceUpdate, envelope *operationsv1.OperationEnvelope) error {
		if basePresence != nil {
			if err := basePresence(ctx, conn, update, envelope); err != nil {
				return err
			}
		}
		return s.HandlePing(ctx, conn, update)
	}

	baseConnect := base.OnConnect
	base.OnConnect = func(ctx context.Context, conn *ws.Connection) error {
		if baseConnect != nil {
			if err := baseConnect(ctx, conn); err != nil {
				return err
			}
		}
		return s.SendRoster(ctx, conn)
	}

	baseDisconnect := base.OnDisconnect
	base.OnDisconnect = func(conn *ws.Connection) {
		if baseDisconnect != nil {
			baseDisconnect(conn)
		}
		s.Clear(context.Background(), conn.DocumentID(), conn.ClientID())
	}

	return base
}
