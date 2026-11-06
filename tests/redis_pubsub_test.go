package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/chat"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"github.com/sagar/grpc-chat-system/pkg/redis"
)

// TestMultiServerBroadcasting verifies that when multiple server instances (pods)
// share a message broker, a message sent to Pod 1 is successfully delivered to clients
// connected to Pod 2 in the same room.
func TestMultiServerBroadcasting(t *testing.T) {
	// Shared broker represents Redis Pub/Sub cluster
	sharedBroker := redis.NewMemoryBroker()
	defer sharedBroker.Close()

	// Spin up two simulated server hubs (Pod 1 and Pod 2)
	hub1 := chat.NewHub("pod-1", sharedBroker, nil)
	defer hub1.Close()

	hub2 := chat.NewHub("pod-2", sharedBroker, nil)
	defer hub2.Close()

	roomID := "architecture-room"

	// Client Alice connects to Pod 1
	alice := chat.NewClient(
		"alice-session",
		"Alice",
		roomID,
		nil,
		32,
		chat.PolicyDropOldest,
		nil,
	)
	hub1.Register(alice)

	// Client Bob connects to Pod 2
	bob := chat.NewClient(
		"bob-session",
		"Bob",
		roomID,
		nil,
		32,
		chat.PolicyDropOldest,
		nil,
	)
	hub2.Register(bob)

	// Alice sends a message via Hub 1
	outgoingMsg := &pb.ChatMessage{
		Id:          uuid.New().String(),
		RoomId:      roomID,
		Sender:      "Alice",
		Content:     "Hello from Pod 1 to Pod 2!",
		TimestampMs: time.Now().UnixMilli(),
		Type:        pb.MessageType_TEXT,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := hub1.HandleIncoming(ctx, alice, outgoingMsg); err != nil {
		t.Fatalf("Failed to handle incoming message on Hub 1: %v", err)
	}

	// Verify Bob on Pod 2 receives Alice's message via broker fanout
	select {
	case receivedMsg := <-bob.SendCh:
		if receivedMsg.Sender != "Alice" {
			t.Fatalf("Expected sender 'Alice', got '%s'", receivedMsg.Sender)
		}
		if receivedMsg.Content != "Hello from Pod 1 to Pod 2!" {
			t.Fatalf("Expected content 'Hello from Pod 1 to Pod 2!', got '%s'", receivedMsg.Content)
		}
		if receivedMsg.Metadata["origin_server"] != "pod-1" {
			t.Fatalf("Expected origin_server metadata 'pod-1', got '%s'", receivedMsg.Metadata["origin_server"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for message to cross from Pod 1 to Bob on Pod 2")
	}

	// Verify Alice on Pod 1 also receives the broadcast
	select {
	case receivedMsg := <-alice.SendCh:
		if receivedMsg.Content != "Hello from Pod 1 to Pod 2!" {
			t.Fatalf("Expected Alice to receive broadcast of her own message, got %s", receivedMsg.Content)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for Alice's local echo")
	}
}

