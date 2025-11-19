package ws

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

const (
	wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

// Authenticator verifies the inbound HTTP request before the connection is
// upgraded to WebSocket.
type Authenticator interface {
	Authenticate(r *http.Request) (ClientIdentity, error)
}

// AuthFunc is an adapter to allow the use of ordinary functions as authenticators.
type AuthFunc func(r *http.Request) (ClientIdentity, error)

// Authenticate implements Authenticator.
func (f AuthFunc) Authenticate(r *http.Request) (ClientIdentity, error) {
	return f(r)
}

// GatewayConfig controls the runtime behaviour of the WebSocket gateway.
type GatewayConfig struct {
	HeartbeatInterval  time.Duration
	HeartbeatTolerance int
	SendBuffer         int
	WriteTimeout       time.Duration
}

// Gateway upgrades HTTP requests into WebSocket connections, validates
// authentication, and wires them into the ConnectionRegistry.
type Gateway struct {
	auth     Authenticator
	registry *ConnectionRegistry
	logger   zerolog.Logger
	hooks    Hooks
	cfg      GatewayConfig
}

// NewGateway creates a Gateway with sane defaults.
func NewGateway(auth Authenticator, registry *ConnectionRegistry, logger zerolog.Logger, hooks Hooks, cfg GatewayConfig) (*Gateway, error) {
	if auth == nil {
		return nil, errors.New("authenticator is required")
	}
	if registry == nil {
		return nil, errors.New("connection registry is required")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.HeartbeatTolerance == 0 {
		cfg.HeartbeatTolerance = 2
	}
	if cfg.SendBuffer == 0 {
		cfg.SendBuffer = 64
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	return &Gateway{auth: auth, registry: registry, logger: logger, hooks: hooks, cfg: cfg}, nil
}

// ServeHTTP implements http.Handler.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	identity, err := g.auth.Authenticate(r)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	documentID := r.URL.Query().Get("document_id")
	if identity.DocumentID != "" {
		documentID = identity.DocumentID
	}
	if documentID == "" {
		http.Error(w, "missing document_id", http.StatusBadRequest)
		return
	}
	if identity.ClientID == "" {
		http.Error(w, "missing client identity", http.StatusUnauthorized)
		return
	}

	if err := g.performUpgrade(w, r, identity, documentID); err != nil {
		g.logger.Error().Err(err).Msg("websocket upgrade failed")
	}
}

func (g *Gateway) performUpgrade(w http.ResponseWriter, r *http.Request, identity ClientIdentity, documentID string) error {
	if !headerContainsToken(r.Header.Get("Connection"), "Upgrade") || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "upgrade headers required", http.StatusBadRequest)
		return errors.New("missing upgrade headers")
	}

	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "unsupported websocket version", http.StatusBadRequest)
		return errors.New("invalid websocket version")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing websocket key", http.StatusBadRequest)
		return errors.New("missing websocket key")
	}

	accept := computeAcceptKey(key)
	protoHeader := selectSubprotocol(r.Header)

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "server does not support hijacking", http.StatusInternalServerError)
		return errors.New("hijacking not supported")
	}

	conn, buf, err := hj.Hijack()
	if err != nil {
		return fmt.Errorf("hijack: %w", err)
	}

	response := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n", accept)
	if protoHeader != "" {
		response += fmt.Sprintf("Sec-WebSocket-Protocol: %s\r\n", protoHeader)
	}
	response += "\r\n"
	if _, err := buf.WriteString(response); err != nil {
		conn.Close()
		return fmt.Errorf("write handshake response: %w", err)
	}
	if err := buf.Flush(); err != nil {
		conn.Close()
		return fmt.Errorf("flush handshake: %w", err)
	}

	childLogger := g.logger.With().Str("document", documentID).Str("client", identity.ClientID).Logger()
	var connection *Connection
	connection = newConnection(conn, identity, documentID, g.registry, childLogger, connectionOptions{
		heartbeatInterval:  g.cfg.HeartbeatInterval,
		heartbeatTolerance: g.cfg.HeartbeatTolerance,
		sendBufferSize:     g.cfg.SendBuffer,
		writeTimeout:       g.cfg.WriteTimeout,
	}, func() {
		g.registry.Unregister(documentID, connection)
	})

	g.registry.Register(documentID, connection)
	childLogger.Info().Msg("websocket connection established")

	go connection.Run(g.hooks)
	return nil
}

func computeAcceptKey(key string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(key) + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func selectSubprotocol(h http.Header) string {
	value := h.Get("Sec-WebSocket-Protocol")
	if value == "" {
		return ""
	}
	// The client may send a comma separated list. We simply echo the first token.
	parts := strings.Split(value, ",")
	return strings.TrimSpace(parts[0])
}

func headerContainsToken(value, token string) bool {
	if value == "" {
		return false
	}
	parts := strings.Split(value, ",")
	for _, part := range parts {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
