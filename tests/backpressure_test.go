package tests

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/chat"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestBackpressureDropOldest proves that a slow consumer that cannot read fast enough
// has its oldest unconsumed messages dropped without blocking the producer or affecting fast consumers.
func TestBackpressureDropOldest(t *testing.T) {
	bufferSize := 5
	totalMessages := 20

	slowClient := chat.NewClient(
		"slow-client-1",
		"SlowConsumer",
		"room-1",
		nil,
		bufferSize,
		chat.PolicyDropOldest,
		nil,
	)

	fastClient := chat.NewClient(
		"fast-client-1",
		"FastConsumer",
		"room-1",
		nil,
		32, // normal buffer size for responsive clients
		chat.PolicyDropOldest,
		nil,
	)

	// Fast consumer background drainer
	var fastReceivedCount int64
	go func() {
		for {
			select {
			case <-fastClient.SendCh:
				atomic.AddInt64(&fastReceivedCount, 1)
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	// Producer sends totalMessages rapidly
	for i := 1; i <= totalMessages; i++ {
		msg := &pb.ChatMessage{
			Id:          fmt.Sprintf("msg-%02d", i),
			RoomId:      "room-1",
			Sender:      "Producer",
			Content:     fmt.Sprintf("Payload #%d", i),
			TimestampMs: time.Now().UnixMilli(),
			Type:        pb.MessageType_TEXT,
		}

		// Enqueue to slow client
		if err := slowClient.Enqueue(msg); err != nil {
			t.Fatalf("Slow client enqueue returned unexpected error: %v", err)
		}

		// Enqueue to fast client
		if err := fastClient.Enqueue(msg); err != nil {
			t.Fatalf("Fast client enqueue returned unexpected error: %v", err)
		}
	}

	// Verify slow consumer:
	// 1. Channel should hold exactly bufferSize messages (5)
	if len(slowClient.SendCh) != bufferSize {
		t.Fatalf("Expected slow consumer channel len %d, got %d", bufferSize, len(slowClient.SendCh))
	}

	// 2. Total dropped messages should equal (totalMessages - bufferSize) = 15
	expectedDropped := int64(totalMessages - bufferSize)
	if atomic.LoadInt64(&slowClient.DroppedCount) != expectedDropped {
		t.Fatalf("Expected dropped count %d, got %d", expectedDropped, atomic.LoadInt64(&slowClient.DroppedCount))
	}

	// 3. The remaining messages in buffer must be the NEWEST ones (16 to 20)
	for i := totalMessages - bufferSize + 1; i <= totalMessages; i++ {
		msg := <-slowClient.SendCh
		expectedID := fmt.Sprintf("msg-%02d", i)
		if msg.Id != expectedID {
			t.Fatalf("Expected message %s in buffer, but got %s", expectedID, msg.Id)
		}
	}

	// 4. Verify fast consumer received all messages without dropping
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&fastReceivedCount) != int64(totalMessages) {
		t.Fatalf("Expected fast consumer to receive all %d messages, got %d", totalMessages, atomic.LoadInt64(&fastReceivedCount))
	}
	if atomic.LoadInt64(&fastClient.DroppedCount) != 0 {
		t.Fatalf("Expected 0 drops for fast consumer, got %d", atomic.LoadInt64(&fastClient.DroppedCount))
	}
}

// TestBackpressureDisconnect proves that the disconnect policy terminates slow consumers
// with a ResourceExhausted gRPC error code when their buffer saturates.
func TestBackpressureDisconnect(t *testing.T) {
	bufferSize := 3

	var disconnectedErr error
	client := chat.NewClient(
		"slow-client-disconnect",
		"SlowRogue",
		"room-2",
		nil,
		bufferSize,
		chat.PolicyDisconnect,
		func(c *chat.Client, err error) {
			disconnectedErr = err
		},
	)

	// Fill buffer to capacity
	for i := 1; i <= bufferSize; i++ {
		err := client.Enqueue(&pb.ChatMessage{
			Id:      fmt.Sprintf("msg-%d", i),
			Content: "fill",
		})
		if err != nil {
			t.Fatalf("Fill message %d should succeed, got %v", i, err)
		}
	}

	// One more message must trigger backpressure disconnect
	overflowMsg := &pb.ChatMessage{
		Id:      uuid.New().String(),
		Content: "overflow",
	}
	err := client.Enqueue(overflowMsg)
	if err == nil {
		t.Fatalf("Expected error when exceeding buffer capacity under disconnect policy")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("Expected gRPC status code ResourceExhausted, got code %v: %v", st.Code(), err)
	}

	if !client.IsClosed() {
		t.Fatalf("Expected client connection to be marked closed")
	}

	if disconnectedErr == nil {
		t.Fatalf("Expected disconnect callback to receive non-nil error")
	}
}

// TestBackpressureDropNewest proves that under drop_newest policy, the incoming message
// is discarded and existing buffered messages remain intact.
func TestBackpressureDropNewest(t *testing.T) {
	bufferSize := 2

	client := chat.NewClient(
		"client-drop-newest",
		"ConsumerNewest",
		"room-3",
		nil,
		bufferSize,
		chat.PolicyDropNewest,
		nil,
	)

	_ = client.Enqueue(&pb.ChatMessage{Id: "msg-1"})
	_ = client.Enqueue(&pb.ChatMessage{Id: "msg-2"})

	// Buffer is full. Next message should be discarded
	err := client.Enqueue(&pb.ChatMessage{Id: "msg-3-dropped"})
	if err != nil {
		t.Fatalf("Enqueue should not return hard error under drop_newest: %v", err)
	}

	if atomic.LoadInt64(&client.DroppedCount) != 1 {
		t.Fatalf("Expected 1 dropped message, got %d", atomic.LoadInt64(&client.DroppedCount))
	}

	// Verify msg-1 and msg-2 are still in channel
	m1 := <-client.SendCh
	m2 := <-client.SendCh
	if m1.Id != "msg-1" || m2.Id != "msg-2" {
		t.Fatalf("Unexpected messages in channel: %s, %s", m1.Id, m2.Id)
	}
}
