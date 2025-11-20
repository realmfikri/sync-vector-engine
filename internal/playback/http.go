package playback

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/types"
)

// HTTPHandler exposes playback via a RESTful endpoint.
type HTTPHandler struct {
	svc    *Service
	logger zerolog.Logger
}

// NewHTTPHandler builds the handler for GET /documents/{id}/state.
func NewHTTPHandler(svc *Service, logger zerolog.Logger) *HTTPHandler {
	return &HTTPHandler{svc: svc, logger: logger}
}

// ServeHTTP implements http.Handler.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "documents" || parts[2] != "state" {
		http.NotFound(w, r)
		return
	}
	docID := parts[1]

	opID := r.URL.Query().Get("at_op")
	atTimeStr := r.URL.Query().Get("at_time")

	var atTime *time.Time
	if atTimeStr != "" {
		parsed, err := time.Parse(time.RFC3339Nano, atTimeStr)
		if err != nil {
			http.Error(w, "invalid at_time", http.StatusBadRequest)
			return
		}
		atTime = &parsed
	}

	resp, err := h.svc.Playback(r.Context(), Request{Document: types.DocumentID(docID), OperationID: types.OperationID(opID), AtTime: atTime})
	if err != nil {
		h.logger.Error().Err(err).Str("document", docID).Msg("playback failed")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "encode response failed", http.StatusInternalServerError)
	}
}
