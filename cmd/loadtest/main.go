package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	proto "google.golang.org/protobuf/proto"

	operationsv1 "github.com/example/sync-vector-engine/internal/pb/proto"
)

type latencySample struct {
	dur time.Duration
}

func main() {
	addr := flag.String("addr", "ws://localhost:8080/ws", "websocket address to target")
	document := flag.String("document", "doc-loadtest", "document id used by all clients")
	clients := flag.Int("clients", 1000, "number of concurrent websocket clients")
	messages := flag.Int("messages", 20, "number of broadcasts to send")
	interval := flag.Duration("interval", 200*time.Millisecond, "delay between broadcasts")
	flag.Parse()

	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := log.With().Str("document", *document).Logger()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}

	latencyCh := make(chan latencySample, *clients**messages)
	var wg sync.WaitGroup

	u, err := url.Parse(*addr)
	if err != nil {
		logger.Fatal().Err(err).Msg("invalid websocket address")
	}

	// create listener clients first
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client-%d", id)
			q := u.Query()
			q.Set("document_id", *document)
			q.Set("client_id", clientID)
			u.RawQuery = q.Encode()
			conn, _, err := dialer.DialContext(ctx, u.String(), nil)
			if err != nil {
				logger.Error().Err(err).Str("client", clientID).Msg("dial failed")
				return
			}
			defer conn.Close()

			go readerLoop(ctx, conn, latencyCh, logger)

			if id == 0 {
				// broadcaster client
				sendTicker := time.NewTicker(*interval)
				defer sendTicker.Stop()
				for j := 0; j < *messages; j++ {
					select {
					case <-ctx.Done():
						return
					case <-sendTicker.C:
						if err := sendPresence(conn, *document, clientID); err != nil {
							logger.Error().Err(err).Msg("failed to send presence")
							return
						}
					}
				}
				stop()
			} else {
				<-ctx.Done()
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(latencyCh)
	}()

	<-ctx.Done()
	report(latencyCh, logger)
}

func sendPresence(conn *websocket.Conn, documentID, clientID string) error {
	env := &operationsv1.OperationEnvelope{
		Body: &operationsv1.OperationEnvelope_Presence{Presence: &operationsv1.PresenceUpdate{
			DocumentId: documentID,
			ClientId:   clientID,
			Metadata: map[string]string{
				"sent_at": time.Now().UTC().Format(time.RFC3339Nano),
			},
		}},
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, data)
}

func readerLoop(ctx context.Context, conn *websocket.Conn, latencies chan<- latencySample, logger zerolog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				logger.Warn().Err(err).Msg("read error")
			}
			return
		}

		var env operationsv1.OperationEnvelope
		if err := proto.Unmarshal(data, &env); err != nil {
			logger.Warn().Err(err).Msg("failed to decode envelope")
			continue
		}
		if presence := env.GetPresence(); presence != nil {
			sentAt := presence.Metadata["sent_at"]
			if sentAt == "" {
				continue
			}
			if ts, err := time.Parse(time.RFC3339Nano, sentAt); err == nil {
				latencies <- latencySample{dur: time.Since(ts)}
			}
		}
	}
}

func report(samples <-chan latencySample, logger zerolog.Logger) {
	var count int
	var total time.Duration
	var max time.Duration
	var under50ms int

	for s := range samples {
		count++
		total += s.dur
		if s.dur > max {
			max = s.dur
		}
		if s.dur < 50*time.Millisecond {
			under50ms++
		}
	}

	if count == 0 {
		fmt.Fprintln(os.Stdout, "no samples collected")
		return
	}

	avg := time.Duration(int64(math.Round(float64(total) / float64(count))))
	pct := (float64(under50ms) / float64(count)) * 100

	fmt.Fprintf(os.Stdout, "Samples: %d\nAvg latency: %s\nMax latency: %s\n<50ms: %.2f%%\n", count, avg, max, pct)
	if pct < 95 {
		logger.Warn().Msg("less than 95% of broadcasts met the 50ms target")
	}
}
