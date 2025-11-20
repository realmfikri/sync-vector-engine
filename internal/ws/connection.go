package ws

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	proto "google.golang.org/protobuf/proto"

	operationsv1 "github.com/example/sync-vector-engine/internal/pb/proto"
)

const (
	opcodeContinuation = 0x0
	opcodeText         = 0x1
	opcodeBinary       = 0x2
	opcodeClose        = 0x8
	opcodePing         = 0x9
	opcodePong         = 0xA

	closeNormalClosure       = 1000
	closeGoingAway           = 1001
	closeUnsupportedData     = 1003
	closePolicyViolation     = 1008
	closeInternalServerError = 1011
	closeTryAgainLater       = 1013
)

var (
	errSendBufferFull = errors.New("send buffer full")
)

type connectionOptions struct {
	heartbeatInterval  time.Duration
	heartbeatTolerance int
	sendBufferSize     int
	writeTimeout       time.Duration
}

// Connection represents an upgraded WebSocket session.
type Connection struct {
	conn      net.Conn
	identity  ClientIdentity
	document  string
	registry  *ConnectionRegistry
	logger    zerolog.Logger
	send      chan outboundMessage
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    chan struct{}

	opts connectionOptions

	lastPong atomic.Int64
	onClose  func()
}

type outboundMessage struct {
	opcode  byte
	payload []byte
}

func newConnection(netConn net.Conn, id ClientIdentity, documentID string, registry *ConnectionRegistry, logger zerolog.Logger, opts connectionOptions, onClose func()) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		conn:     netConn,
		identity: id,
		document: documentID,
		registry: registry,
		logger:   logger,
		send:     make(chan outboundMessage, opts.sendBufferSize),
		ctx:      ctx,
		cancel:   cancel,
		closed:   make(chan struct{}),
		opts:     opts,
		onClose:  onClose,
	}
	c.lastPong.Store(time.Now().UnixNano())
	return c
}

// DocumentID returns the bound document identifier.
func (c *Connection) DocumentID() string { return c.document }

// ClientID returns the authenticated client identifier.
func (c *Connection) ClientID() string { return c.identity.ClientID }

// Metadata exposes the caller-supplied client metadata, if any.
func (c *Connection) Metadata() map[string]string { return c.identity.Metadata }

// Context exposes the lifecycle context for hooks.
func (c *Connection) Context() context.Context { return c.ctx }

// Registry returns the shared connection registry so hooks can publish events.
func (c *Connection) Registry() *ConnectionRegistry { return c.registry }

// SendEnvelope marshals the provided envelope before enqueueing it for delivery.
func (c *Connection) SendEnvelope(env *operationsv1.OperationEnvelope) error {
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	return c.SendBinary(data)
}

// SendBinary enqueues a binary payload for the writer goroutine.
func (c *Connection) SendBinary(payload []byte) error {
	if payload == nil {
		payload = []byte{}
	}
	msg := outboundMessage{opcode: opcodeBinary, payload: payload}
	select {
	case c.send <- msg:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		c.logger.Warn().Str("document", c.document).Str("client", c.identity.ClientID).Msg("send buffer full; closing connection")
		c.closeWithFrame(closeTryAgainLater, "backpressure")
		return errSendBufferFull
	}
}

// Run starts the read/write pumps until the connection is closed.
func (c *Connection) Run(hooks Hooks) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.writeLoop()
	}()
	go func() {
		defer wg.Done()
		c.heartbeatLoop()
	}()

	if err := c.readLoop(hooks); err != nil {
		c.logger.Debug().Err(err).Msg("read loop exited")
	}
	c.Close()
	wg.Wait()
}

func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		close(c.send)
		_ = c.conn.Close()
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	})
}

func (c *Connection) readLoop(hooks Hooks) error {
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		opcode, payload, err := readFrame(c.conn)
		if err != nil {
			return err
		}

		switch opcode {
		case opcodeBinary:
			if err := c.handleBinary(payload, hooks); err != nil {
				c.closeWithFrame(closePolicyViolation, err.Error())
				return err
			}
		case opcodeText:
			c.closeWithFrame(closeUnsupportedData, "text frames not supported")
			return fmt.Errorf("text frames unsupported")
		case opcodeClose:
			c.closeWithFrame(closeNormalClosure, "bye")
			return nil
		case opcodePing:
			_ = c.enqueueControl(opcodePong, payload)
		case opcodePong:
			c.lastPong.Store(time.Now().UnixNano())
		case opcodeContinuation:
			return fmt.Errorf("fragmented frames not supported")
		default:
			return fmt.Errorf("unsupported opcode %d", opcode)
		}
	}
}

func (c *Connection) handleBinary(payload []byte, hooks Hooks) error {
	var envelope operationsv1.OperationEnvelope
	if err := proto.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}

	if presence := envelope.GetPresence(); presence != nil {
		if hooks.OnPresence != nil {
			if err := hooks.OnPresence(c.ctx, c, presence, &envelope); err != nil {
				return err
			}
		}
		return nil
	}

	if cursor := envelope.GetCursor(); cursor != nil {
		if hooks.OnPresence != nil {
			update := &operationsv1.PresenceUpdate{
				DocumentId: c.document,
				ClientId:   cursor.ClientId,
			}
			if pos := cursor.GetPosition(); pos != nil {
				update.Line = pos.Line
				update.Column = pos.Column
			}
			if err := hooks.OnPresence(c.ctx, c, update, &envelope); err != nil {
				return err
			}
		}
		return nil
	}

	if hooks.OnOperation != nil {
		if err := hooks.OnOperation(c.ctx, c, &envelope); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connection) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			if err := writeFrame(c.conn, msg.opcode, msg.payload, c.opts.writeTimeout); err != nil {
				c.logger.Debug().Err(err).Msg("write loop error")
				c.closeWithFrame(closeInternalServerError, "write error")
				return
			}
		}
	}
}

func (c *Connection) heartbeatLoop() {
	if c.opts.heartbeatInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.opts.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.enqueueControl(opcodePing, nil); err != nil {
				c.logger.Debug().Err(err).Msg("heartbeat ping failed")
				c.closeWithFrame(closeGoingAway, "ping failed")
				return
			}
			if c.opts.heartbeatTolerance > 0 {
				last := time.Unix(0, c.lastPong.Load())
				allowed := c.opts.heartbeatInterval * time.Duration(c.opts.heartbeatTolerance)
				if time.Since(last) > allowed {
					c.logger.Debug().Msg("heartbeat tolerance exceeded")
					c.closeWithFrame(closeGoingAway, "missed heartbeats")
					return
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Connection) closeWithFrame(code int, reason string) {
	payload := encodeClosePayload(code, reason)
	_ = c.enqueueControl(opcodeClose, payload)
}

func (c *Connection) enqueueControl(opcode byte, payload []byte) error {
	msg := outboundMessage{opcode: opcode, payload: payload}
	select {
	case c.send <- msg:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		return errSendBufferFull
	}
}

type Hooks struct {
	OnOperation  OperationHook
	OnPresence   PresenceHook
	OnConnect    ConnectHook
	OnDisconnect DisconnectHook
}

type OperationHook func(ctx context.Context, conn *Connection, envelope *operationsv1.OperationEnvelope) error
type PresenceHook func(ctx context.Context, conn *Connection, update *operationsv1.PresenceUpdate, envelope *operationsv1.OperationEnvelope) error
type ConnectHook func(ctx context.Context, conn *Connection) error
type DisconnectHook func(conn *Connection)

type ClientIdentity struct {
	ClientID   string
	DocumentID string
	Metadata   map[string]string
}

func encodeClosePayload(code int, reason string) []byte {
	if len(reason) > 123 {
		reason = reason[:123]
	}
	payload := make([]byte, 2+len(reason))
	payload[0] = byte(code >> 8)
	payload[1] = byte(code)
	copy(payload[2:], []byte(reason))
	return payload
}
