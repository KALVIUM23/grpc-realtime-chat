package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/sagar/grpc-chat-system/pkg/pb"
)

// MessageBroker defines the interface for cross-pod chat message fanout.
type MessageBroker interface {
	Publish(ctx context.Context, channel string, msg *pb.ChatMessage) error
	Subscribe(ctx context.Context, channel string, handler func(msg *pb.ChatMessage)) error
	Close() error
}

// MemoryBroker is an in-memory implementation of MessageBroker.
// It is used for unit testing, integration simulations, and local fallbacks.
type MemoryBroker struct {
	mu          sync.RWMutex
	subscribers map[string][]func(msg *pb.ChatMessage)
	closed      bool
}

// NewMemoryBroker creates a new in-memory message broker.
func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{
		subscribers: make(map[string][]func(msg *pb.ChatMessage)),
	}
}

// Publish broadcasts the message to all local subscribers of the channel.
func (m *MemoryBroker) Publish(ctx context.Context, channel string, msg *pb.ChatMessage) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return fmt.Errorf("memory broker is closed")
	}

	subs, ok := m.subscribers[channel]
	if !ok {
		return nil
	}

	for _, sub := range subs {
		// Run handler in goroutine to simulate asynchronous decoupled delivery
		go sub(msg)
	}

	return nil
}

// Subscribe registers a message callback handler for the specified channel.
func (m *MemoryBroker) Subscribe(ctx context.Context, channel string, handler func(msg *pb.ChatMessage)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("memory broker is closed")
	}

	m.subscribers[channel] = append(m.subscribers[channel], handler)
	return nil
}

// Close closes the in-memory broker.
func (m *MemoryBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.subscribers = make(map[string][]func(msg *pb.ChatMessage))
	return nil
}

