package tests

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagar/grpc-chat-system/pkg/moderation"
	"github.com/sagar/grpc-chat-system/pkg/pb"
)

type mockClassifier struct {
	customFn func(ctx context.Context, text string) (bool, string, error)
}

func (m *mockClassifier) CheckSafety(ctx context.Context, text string) (bool, string, error) {
	if m.customFn != nil {
		return m.customFn(ctx, text)
	}
	if strings.Contains(strings.ToLower(text), "toxic") || strings.Contains(strings.ToLower(text), "violation") {
		return false, "toxic_content", nil
	}
	return true, "safe", nil
}

func TestAsyncModerationPipeline(t *testing.T) {
	classifier := &mockClassifier{}

	var flaggedCount int64
	var lastFlaggedID string

	pipeline := moderation.NewAsyncModerationPipeline(
		classifier,
		100,
		2,
		func(originalMsg *pb.ChatMessage, reason string) {
			atomic.AddInt64(&flaggedCount, 1)
			lastFlaggedID = originalMsg.Id
		},
	)
	defer pipeline.Close()

	// 1. Send safe message
	safeMsg := &pb.ChatMessage{
		Id:      "msg-safe-001",
		Content: "Good morning team!",
		Type:    pb.MessageType_TEXT,
	}
	pipeline.Enqueue(safeMsg)

	// 2. Send violating message
	badMsg := &pb.ChatMessage{
		Id:      "msg-bad-002",
		Content: "This message has toxic language violation",
		Type:    pb.MessageType_TEXT,
	}
	pipeline.Enqueue(badMsg)

	// Wait briefly for async workers to finish processing
	time.Sleep(200 * time.Millisecond)

	processed, flagged := pipeline.GetStats()
	if processed != 2 {
		t.Fatalf("Expected 2 processed messages, got %d", processed)
	}
	if flagged != 1 || atomic.LoadInt64(&flaggedCount) != 1 {
		t.Fatalf("Expected 1 flagged message, got %d", flagged)
	}
	if lastFlaggedID != "msg-bad-002" {
		t.Fatalf("Expected last flagged ID to be 'msg-bad-002', got '%s'", lastFlaggedID)
	}
}

func TestModerationFailOpenOnError(t *testing.T) {
	// Simulate Ollama downtime / connection failure
	failingClassifier := &mockClassifier{
		customFn: func(ctx context.Context, text string) (bool, string, error) {
			return true, "ollama_down", fmt.Errorf("connection refused: dial tcp 127.0.0.1:11434")
		},
	}

	var flaggedCount int64
	pipeline := moderation.NewAsyncModerationPipeline(
		failingClassifier,
		50,
		1,
		func(originalMsg *pb.ChatMessage, reason string) {
			atomic.AddInt64(&flaggedCount, 1)
		},
	)
	defer pipeline.Close()

	msg := &pb.ChatMessage{
		Id:      "msg-error-001",
		Content: "Testing connection error resilience",
		Type:    pb.MessageType_TEXT,
	}

	pipeline.Enqueue(msg)
	time.Sleep(150 * time.Millisecond)

	processed, flagged := pipeline.GetStats()
	if processed != 1 {
		t.Fatalf("Expected 1 processed message, got %d", processed)
	}
	if flagged != 0 {
		t.Fatalf("Expected 0 flagged messages on connection error (fail-open), got %d", flagged)
	}
}

