package chat

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/sagar/grpc-chat-system/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BackpressurePolicy defines the server action when a client buffer is full.
type BackpressurePolicy string

const (
	// PolicyDropOldest discards the oldest unconsumed message in the client buffer to fit the latest message.
	PolicyDropOldest BackpressurePolicy = "drop_oldest"

	// PolicyDisconnect immediately disconnects slow consumers with a ResourceExhausted gRPC error.
	PolicyDisconnect BackpressurePolicy = "disconnect"

	// PolicyDropNewest discards the incoming message if the client buffer is full.
	PolicyDropNewest BackpressurePolicy = "drop_newest"
)

// Client represents an active gRPC streaming connection on this server instance.
type Client struct {
	ID                 string
	Username           string
	RoomID             string
	Stream             pb.ChatService_ChatStreamServer
	SendCh             chan *pb.ChatMessage
	BufferSize         int
	Policy             BackpressurePolicy
	DroppedCount       int64
	SentCount          int64
	done               chan struct{}
	closeOnce          sync.Once
	mu                 sync.RWMutex
	disconnectCallback func(c *Client, err error)
}

// NewClient initializes a new connected client.
func NewClient(
	id string,
	username string,
	roomID string,
	stream pb.ChatService_ChatStreamServer,
	bufferSize int,
	policy BackpressurePolicy,
	onDisconnect func(c *Client, err error),
) *Client {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	if policy == "" {
		policy = PolicyDropOldest
	}

	return &Client{
		ID:                 id,
		Username:           username,
		RoomID:             roomID,
		Stream:             stream,
		SendCh:             make(chan *pb.ChatMessage, bufferSize),
		BufferSize:         bufferSize,
		Policy:             policy,
		done:               make(chan struct{}),
		disconnectCallback: onDisconnect,
	}
}

// Enqueue delivers a message to the client's outbound channel, executing backpressure if saturated.
func (c *Client) Enqueue(msg *pb.ChatMessage) error {
	select {
	case <-c.done:
		return errors.New("client connection closed")
	default:
	}

	switch c.Policy {
	case PolicyDropOldest:
		// Attempt non-blocking send first
		select {
		case c.SendCh <- msg:
			return nil
		default:
			// Buffer full! Slow consumer detected.
			// Drain the oldest message from channel
			select {
			case oldest := <-c.SendCh:
				atomic.AddInt64(&c.DroppedCount, 1)
				log.Printf("[Backpressure:DropOldest] Client %s (%s) buffer full! Evicted oldest msg %s (total dropped: %d)",
					c.Username, c.ID, oldest.Id, atomic.LoadInt64(&c.DroppedCount))
			default:
				// Channel might have been drained concurrently by write pump
			}

			// Try enqueuing the new message
			select {
			case c.SendCh <- msg:
				return nil
			default:
				atomic.AddInt64(&c.DroppedCount, 1)
				return errors.New("client buffer still saturated after eviction")
			}
		}

	case PolicyDisconnect:
		select {
		case c.SendCh <- msg:
			return nil
		default:
			atomic.AddInt64(&c.DroppedCount, 1)
			err := status.Errorf(codes.ResourceExhausted,
				"client %s (%s) disconnected: buffer saturated (%d/%d msgs)",
				c.Username, c.ID, c.BufferSize, c.BufferSize)
			log.Printf("[Backpressure:Disconnect] %v", err)
			c.CloseWithError(err)
			return err
		}

	case PolicyDropNewest:
		select {
		case c.SendCh <- msg:
			return nil
		default:
			atomic.AddInt64(&c.DroppedCount, 1)
			log.Printf("[Backpressure:DropNewest] Client %s (%s) buffer full! Dropped incoming msg %s",
				c.Username, c.ID, msg.Id)
			return nil
		}

	default:
		return fmt.Errorf("unknown backpressure policy: %s", c.Policy)
	}
}

// StartWritePump runs the loop that pulls from SendCh and writes to gRPC stream.
func (c *Client) StartWritePump() error {
	for {
		select {
		case <-c.done:
			return nil
		case msg, ok := <-c.SendCh:
			if !ok {
				return nil
			}

			if c.Stream != nil {
				if err := c.Stream.Send(msg); err != nil {
					log.Printf("[ClientWritePump] Send error for client %s: %v", c.Username, err)
					c.CloseWithError(err)
					return err
				}
				atomic.AddInt64(&c.SentCount, 1)
			}
		}
	}
}

// Close gracefully closes the client connection.
func (c *Client) Close() {
	c.CloseWithError(nil)
}

// CloseWithError closes the client connection and invokes the disconnect callback.
func (c *Client) CloseWithError(err error) {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.disconnectCallback != nil {
			c.disconnectCallback(c, err)
		}
	})
}

// IsClosed checks if the client connection is closed.
func (c *Client) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

