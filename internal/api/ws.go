package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/metrics"
)

const (
	wsWriteWait      = 10 * time.Second
	wsPongWait       = 60 * time.Second
	wsPingPeriod     = 50 * time.Second
	wsMaxMessageSize = 4096
	wsSendBuffer     = 256
	wsMaxSubscribed  = 1000
)

// Hub fans status events out to WebSocket clients.
//
// Clients connect to GET /ws and choose what they want to receive, either with
// query parameters (?notificationId=..&batchId=..&channel=..) or by sending
// subscribe messages:
//
//	{"action":"subscribe","notificationIds":["..."],"batchIds":["..."]}
//
// A client without any filter receives every event (useful for dashboards).
// Slow clients whose buffer fills up are disconnected rather than slowing the hub.
type Hub struct {
	log      *slog.Logger
	metrics  *metrics.Metrics
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	send chan []byte

	mu            sync.RWMutex
	notifications map[uuid.UUID]struct{}
	batches       map[uuid.UUID]struct{}
	channel       domain.Channel
}

// NewHub creates a hub. allowedOrigins empty means any origin is accepted.
func NewHub(log *slog.Logger, m *metrics.Metrics, allowedOrigins []string) *Hub {
	h := &Hub{log: log, metrics: m, clients: map[*wsClient]struct{}{}}
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			if len(allowedOrigins) == 0 {
				return true
			}
			origin := r.Header.Get("Origin")
			for _, o := range allowedOrigins {
				if strings.EqualFold(o, origin) {
					return true
				}
			}
			return false
		},
	}
	return h
}

// Run feeds the hub from subscribe until ctx is done.
func (h *Hub) Run(ctx context.Context, subscribe func(context.Context, func(events.Event)) error) {
	for ctx.Err() == nil {
		if err := subscribe(ctx, h.Broadcast); err != nil && ctx.Err() == nil {
			h.log.Warn("event subscription failed, retrying", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}
}

// Broadcast delivers e to every interested client.
func (h *Hub) Broadcast(e events.Event) {
	payload, err := json.Marshal(e)
	if err != nil {
		return
	}
	h.mu.RLock()
	var slow []*wsClient
	for c := range h.clients {
		if !c.wants(e) {
			continue
		}
		select {
		case c.send <- payload:
		default:
			slow = append(slow, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range slow {
		h.remove(c)
	}
}

// Clients returns the number of connected clients.
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (c *wsClient) wants(e events.Event) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.channel != "" && c.channel != e.Channel {
		return false
	}
	if len(c.notifications) == 0 && len(c.batches) == 0 {
		return true
	}
	if _, ok := c.notifications[e.NotificationID]; ok {
		return true
	}
	if e.BatchID != nil {
		if _, ok := c.batches[*e.BatchID]; ok {
			return true
		}
	}
	return false
}

// subscribe adds ids; returns false when the per-client limit would be exceeded.
func (c *wsClient) subscribe(notifications, batches []string) (bool, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var invalid []string
	add := func(set map[uuid.UUID]struct{}, ids []string) bool {
		for _, raw := range ids {
			id, err := uuid.Parse(strings.TrimSpace(raw))
			if err != nil {
				invalid = append(invalid, raw)
				continue
			}
			if len(c.notifications)+len(c.batches) >= wsMaxSubscribed {
				return false
			}
			set[id] = struct{}{}
		}
		return true
	}
	ok := add(c.notifications, notifications) && add(c.batches, batches)
	return ok, invalid
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.WebSocketClients.Inc()
	}
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
		if h.metrics != nil {
			h.metrics.WebSocketClients.Dec()
		}
	}
	h.mu.Unlock()
}

type wsCommand struct {
	Action          string   `json:"action"`
	NotificationIDs []string `json:"notificationIds"`
	BatchIDs        []string `json:"batchIds"`
}

// ServeWS upgrades the connection and starts the read/write pumps.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c := &wsClient{
		send:          make(chan []byte, wsSendBuffer),
		notifications: map[uuid.UUID]struct{}{},
		batches:       map[uuid.UUID]struct{}{},
	}
	if ch := q.Get("channel"); ch != "" {
		parsed, err := domain.ParseChannel(ch)
		if err != nil {
			writeErrorCode(w, r, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		c.channel = parsed
	}
	if _, invalid := c.subscribe(q["notificationId"], q["batchId"]); len(invalid) > 0 {
		writeErrorCode(w, r, http.StatusBadRequest, "invalid_request", "notificationId and batchId must be UUIDs")
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // the upgrader already wrote an HTTP error
	}
	h.add(c)
	welcome, _ := json.Marshal(map[string]any{"type": "connected", "message": "subscribed to notification status events"})
	h.sendTo(c, welcome)

	go h.writePump(conn, c)
	h.readPump(conn, c)
}

func (h *Hub) readPump(conn *websocket.Conn, c *wsClient) {
	defer func() {
		h.remove(c)
		_ = conn.Close()
	}()
	conn.SetReadLimit(wsMaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(wsPongWait)) })
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var cmd wsCommand
		reply := map[string]any{"type": "ack"}
		if err := json.Unmarshal(data, &cmd); err != nil || cmd.Action != "subscribe" {
			reply = map[string]any{"type": "error", "message": `expected {"action":"subscribe","notificationIds":[],"batchIds":[]}`}
		} else if ok, invalid := c.subscribe(cmd.NotificationIDs, cmd.BatchIDs); !ok {
			reply = map[string]any{"type": "error", "message": "subscription limit reached"}
		} else if len(invalid) > 0 {
			reply = map[string]any{"type": "error", "message": "invalid ids ignored", "invalid": invalid}
		}
		b, _ := json.Marshal(reply)
		h.sendTo(c, b)
	}
}

// sendTo queues a message for c unless c was already removed (its send channel
// is closed by remove, which holds the write lock).
func (h *Hub) sendTo(c *wsClient, b []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if _, ok := h.clients[c]; !ok {
		return
	}
	select {
	case c.send <- b:
	default:
	}
}

func (h *Hub) writePump(conn *websocket.Conn, c *wsClient) {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		_ = conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if !ok {
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
