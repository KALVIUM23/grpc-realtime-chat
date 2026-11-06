package redis

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"google.golang.org/protobuf/proto"
)

// RedisBroker implements MessageBroker backed by a real Redis instance using Pub/Sub.
type RedisBroker struct {
	client     *redis.Client
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	pubsubs    []*redis.PubSub
	isClosed   bool
}

// NewRedisBroker creates and verifies connection to a Redis server.
func NewRedisBroker(addr string, password string, db int) (*RedisBroker, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:            addr,
		Password:        password,
		DB:              db,
		MaxRetries:      5,
		MinRetryBackoff: 200 * time.Millisecond,
		MaxRetryBackoff: 2 * time.Second,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to ping redis at %s: %w", addr, err)
	}

	log.Printf("[RedisBroker] Connected successfully to Redis at %s", addr)

	return &RedisBroker{
		client:  rdb,
		ctx:     ctx,
		cancel:  cancel,
		pubsubs: make([]*redis.PubSub, 0),
	}, nil
}

// Publish serializes the chat message to protobuf bytes and publishes it to Redis.
func (r *RedisBroker) Publish(ctx context.Context, channel string, msg *pb.ChatMessage) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal proto message: %w", err)
	}

	if err := r.client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("failed to publish message to redis channel %s: %w", channel, err)
	}

	return nil
}

// Subscribe starts a listener loop on the specified Redis channel.
func (r *RedisBroker) Subscribe(ctx context.Context, channel string, handler func(msg *pb.ChatMessage)) error {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()
		return fmt.Errorf("redis broker is closed")
	}

	pubsub := r.client.Subscribe(ctx, channel)
	r.pubsubs = append(r.pubsubs, pubsub)
	r.mu.Unlock()

	// Wait for subscription confirmation
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return fmt.Errorf("failed to confirm subscription to channel %s: %w", channel, err)
	}

	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case <-r.ctx.Done():
				return
			case redisMsg, ok := <-ch:
				if !ok {
					log.Printf("[RedisBroker] Subscription channel closed for %s", channel)
					return
				}

				var msg pb.ChatMessage
				if err := proto.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
					log.Printf("[RedisBroker] Failed to unmarshal message from channel %s: %v", channel, err)
					continue
				}

				handler(&msg)
			}
		}
	}()

	return nil
}

// Close closes all active subscriptions and the Redis client connection.
func (r *RedisBroker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed {
		return nil
	}
	r.isClosed = true

	r.cancel()

	for _, ps := range r.pubsubs {
		_ = ps.Close()
	}

	return r.client.Close()
}

