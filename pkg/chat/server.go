package chat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements pb.ChatServiceServer.
type Server struct {
	pb.UnimplementedChatServiceServer
	hub               *Hub
	bufferSize        int
	backpressurePolicy BackpressurePolicy
}

// NewServer initializes a new ChatServiceServer.
func NewServer(hub *Hub, bufferSize int, policy BackpressurePolicy) *Server {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	if policy == "" {
		policy = PolicyDropOldest
	}

	return &Server{
		hub:                hub,
		bufferSize:         bufferSize,
		backpressurePolicy: policy,
	}
}

// ChatStream handles bidirectional streaming for real-time room communication.
func (s *Server) ChatStream(stream pb.ChatService_ChatStreamServer) error {
	ctx := stream.Context()

	// 1. Wait for initial connection/handshake message
	initialMsg, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return status.Errorf(codes.InvalidArgument, "failed to receive initial join message: %v", err)
	}

	roomID := initialMsg.RoomId
	if roomID == "" {
		roomID = "general"
	}
	username := initialMsg.Sender
	if username == "" {
		username = fmt.Sprintf("guest-%s", uuid.New().String()[:4])
	}

	clientID := uuid.New().String()

	var writeErrMu sync.Mutex
	var writeErr error

	// Create client session
	client := NewClient(
		clientID,
		username,
		roomID,
		stream,
		s.bufferSize,
		s.backpressurePolicy,
		func(c *Client, err error) {
			if err != nil {
				writeErrMu.Lock()
				writeErr = err
				writeErrMu.Unlock()
			}
		},
	)

	// Register with Hub
	s.hub.Register(client)

	// Send recent room history to newly joined client
	recentHistory := s.hub.GetHistory(roomID, 15)
	for _, historyMsg := range recentHistory {
		_ = client.Enqueue(historyMsg)
	}

	// Broadcast JOIN announcement across the room
	joinNotice := &pb.ChatMessage{
		Id:          uuid.New().String(),
		RoomId:      roomID,
		Sender:      "SYSTEM",
		Content:     fmt.Sprintf("%s has entered the room.", username),
		TimestampMs: time.Now().UnixMilli(),
		Type:        pb.MessageType_JOIN,
	}
	_ = s.hub.Publish(ctx, joinNotice)

	// Start write pump in dedicated goroutine
	writePumpDone := make(chan error, 1)
	go func() {
		writePumpDone <- client.StartWritePump()
	}()

	// Read pump loop
	readErr := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				msg, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}

				// Enforce message origin attributes
				msg.Sender = username
				msg.RoomId = roomID

				if err := s.hub.HandleIncoming(ctx, client, msg); err != nil {
					log.Printf("[Server] Error handling incoming msg from %s: %v", username, err)
				}
			}
		}
	}()

	// Client has exited or disconnected: clean up
	client.Close()
	<-writePumpDone

	s.hub.Unregister(client)

	// Broadcast LEAVE announcement
	leaveNotice := &pb.ChatMessage{
		Id:          uuid.New().String(),
		RoomId:      roomID,
		Sender:      "SYSTEM",
		Content:     fmt.Sprintf("%s has left the room.", username),
		TimestampMs: time.Now().UnixMilli(),
		Type:        pb.MessageType_LEAVE,
	}
	// Use background context for cleanup message because stream context might already be cancelled
	_ = s.hub.Publish(context.Background(), leaveNotice)

	writeErrMu.Lock()
	defer writeErrMu.Unlock()
	if writeErr != nil {
		return writeErr
	}

	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return status.Errorf(codes.Canceled, "stream ended: %v", readErr)
	}

	return nil
}

// SendMessage allows unary message submission.
func (s *Server) SendMessage(ctx context.Context, msg *pb.ChatMessage) (*pb.SendResponse, error) {
	if msg.RoomId == "" {
		return &pb.SendResponse{Success: false, ErrorMessage: "room_id is required"}, status.Error(codes.InvalidArgument, "room_id is required")
	}
	if msg.Sender == "" {
		msg.Sender = "anonymous"
	}
	if msg.Id == "" {
		msg.Id = uuid.New().String()
	}
	if msg.TimestampMs == 0 {
		msg.TimestampMs = time.Now().UnixMilli()
	}

	if err := s.hub.Publish(ctx, msg); err != nil {
		return &pb.SendResponse{Success: false, ErrorMessage: err.Error()}, status.Errorf(codes.Internal, "failed to publish: %v", err)
	}

	return &pb.SendResponse{
		Success:   true,
		MessageId: msg.Id,
	}, nil
}

// GetRoomHistory returns stored messages for a specific room.
func (s *Server) GetRoomHistory(ctx context.Context, req *pb.RoomHistoryRequest) (*pb.RoomHistoryResponse, error) {
	if req.RoomId == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id is required")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}

	messages := s.hub.GetHistory(req.RoomId, limit)
	return &pb.RoomHistoryResponse{
		Messages: messages,
	}, nil
}

// HealthCheck responds to Kubernetes liveness and readiness probes.
func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Status: pb.HealthCheckResponse_SERVING,
	}, nil
}

