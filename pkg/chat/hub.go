package chat

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/moderation"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"github.com/sagar/grpc-chat-system/pkg/redis"
)

// Hub manages active chat rooms and coordinates cross-pod messaging.
type Hub struct {
	mu             sync.RWMutex
	rooms          map[string]map[string]*Client // roomID -> clientID -> *Client
	history        map[string][]*pb.ChatMessage  // roomID -> recent messages
	historyLimit   int
	broker         redis.MessageBroker
	moderationPipe *moderation.AsyncModerationPipeline
	serverID       string
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewHub initializes a Hub with a message broker and optional moderation pipeline.
func NewHub(serverID string, broker redis.MessageBroker, moderationPipe *moderation.AsyncModerationPipeline) *Hub {
	if serverID == "" {
		serverID = uuid.New().String()[:8]
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &Hub{
		rooms:          make(map[string]map[string]*Client),
		history:        make(map[string][]*pb.ChatMessage),
		historyLimit:   50,
		broker:         broker,
		moderationPipe: moderationPipe,
		serverID:       serverID,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Subscribe to the global broadcast channel in Redis
	if broker != nil {
		err := broker.Subscribe(ctx, "chat:global_broadcast", func(msg *pb.ChatMessage) {
			h.BroadcastLocal(msg)
		})
		if err != nil {
			log.Printf("[Hub-%s] Warning: Failed to subscribe to global broadcast: %v", serverID, err)
		} else {
			log.Printf("[Hub-%s] Subscribed to Redis channel 'chat:global_broadcast'", serverID)
		}
	}

	return h
}

// Register adds a client to a room on this server instance.
func (h *Hub) Register(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	roomClients, exists := h.rooms[client.RoomID]
	if !exists {
		roomClients = make(map[string]*Client)
		h.rooms[client.RoomID] = roomClients
	}

	roomClients[client.ID] = client
	log.Printf("[Hub-%s] Registered client %s (%s) in room '%s'. Total room clients on this pod: %d",
		h.serverID, client.Username, client.ID, client.RoomID, len(roomClients))
}

// Unregister removes a client from their room.
func (h *Hub) Unregister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if roomClients, exists := h.rooms[client.RoomID]; exists {
		delete(roomClients, client.ID)
		log.Printf("[Hub-%s] Unregistered client %s (%s) from room '%s'. Remaining on this pod: %d",
			h.serverID, client.Username, client.ID, client.RoomID, len(roomClients))
		if len(roomClients) == 0 {
			delete(h.rooms, client.RoomID)
		}
	}
}

// BroadcastLocal sends a message only to clients connected directly to this server instance.
func (h *Hub) BroadcastLocal(msg *pb.ChatMessage) {
	h.mu.Lock()
	// Record to history if it is a standard or moderation message
	if msg.Type == pb.MessageType_TEXT || msg.Type == pb.MessageType_MODERATION_FLAGGED {
		roomHistory := h.history[msg.RoomId]
		if len(roomHistory) >= h.historyLimit {
			roomHistory = roomHistory[1:]
		}
		h.history[msg.RoomId] = append(roomHistory, msg)
	}

	roomClients, exists := h.rooms[msg.RoomId]
	if !exists || len(roomClients) == 0 {
		h.mu.Unlock()
		return
	}

	// Copy pointers under lock to avoid holding the lock during channel enqueues
	clients := make([]*Client, 0, len(roomClients))
	for _, c := range roomClients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	for _, c := range clients {
		if c.IsClosed() {
			continue
		}
		// Non-blocking enqueue with backpressure policy
		if err := c.Enqueue(msg); err != nil {
			log.Printf("[Hub-%s] Failed to deliver msg %s to %s: %v", h.serverID, msg.Id, c.Username, err)
		}
	}
}

// Publish broadcasts the message across all server pods via Redis Pub/Sub.
func (h *Hub) Publish(ctx context.Context, msg *pb.ChatMessage) error {
	if msg.Id == "" {
		msg.Id = uuid.New().String()
	}
	if msg.TimestampMs == 0 {
		msg.TimestampMs = time.Now().UnixMilli()
	}
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]string)
	}
	msg.Metadata["origin_server"] = h.serverID

	if h.broker != nil {
		return h.broker.Publish(ctx, "chat:global_broadcast", msg)
	}

	// If no external broker, fallback to local broadcast directly
	h.BroadcastLocal(msg)
	return nil
}

// HandleIncoming receives a message from a connected client, fans it out to Redis,
// and submits it to the async LLM moderation pipeline.
func (h *Hub) HandleIncoming(ctx context.Context, client *Client, msg *pb.ChatMessage) error {
	msg.Sender = client.Username
	msg.RoomId = client.RoomID
	if msg.Id == "" {
		msg.Id = uuid.New().String()
	}
	msg.TimestampMs = time.Now().UnixMilli()

	// 1. Immediately publish to broker so other participants receive the message (< 5ms)
	if err := h.Publish(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish chat message: %w", err)
	}

	// 2. Asynchronously submit to moderation pipeline (non-blocking)
	if h.moderationPipe != nil && msg.Type == pb.MessageType_TEXT {
		h.moderationPipe.Enqueue(msg)
	}

	return nil
}

// GetHistory returns recent messages for a room.
func (h *Hub) GetHistory(roomID string, limit int32) []*pb.ChatMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()

	hist := h.history[roomID]
	if limit <= 0 || int(limit) >= len(hist) {
		result := make([]*pb.ChatMessage, len(hist))
		copy(result, hist)
		return result
	}

	start := len(hist) - int(limit)
	result := make([]*pb.ChatMessage, limit)
	copy(result, hist[start:])
	return result
}

// Close gracefully terminates the hub and active subscriptions.
func (h *Hub) Close() error {
	h.cancel()

	h.mu.Lock()
	for _, roomClients := range h.rooms {
		for _, client := range roomClients {
			client.Close()
		}
	}
	h.mu.Unlock()

	if h.broker != nil {
		return h.broker.Close()
	}
	return nil
}

