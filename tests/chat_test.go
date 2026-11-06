package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagar/grpc-chat-system/pkg/chat"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"github.com/sagar/grpc-chat-system/pkg/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupTestGRPCServer(t *testing.T) (pb.ChatServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	broker := redis.NewMemoryBroker()
	hub := chat.NewHub("test-pod-1", broker, nil)
	chatServer := chat.NewServer(hub, 32, chat.PolicyDropOldest)

	pb.RegisterChatServiceServer(s, chatServer)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Server exited with error: %v", err)
		}
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := pb.NewChatServiceClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
		lis.Close()
		hub.Close()
	}

	return client, cleanup
}

func TestHealthCheck(t *testing.T) {
	client, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{Service: "chat"})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}

	if resp.Status != pb.HealthCheckResponse_SERVING {
		t.Fatalf("Expected status SERVING, got %v", resp.Status)
	}
}

func TestUnarySendMessageAndHistory(t *testing.T) {
	client, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sendResp, err := client.SendMessage(ctx, &pb.ChatMessage{
		RoomId:  "engineering",
		Sender:  "Alice",
		Content: "Hello engineering team!",
		Type:    pb.MessageType_TEXT,
	})
	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}
	if !sendResp.Success || sendResp.MessageId == "" {
		t.Fatalf("SendMessage returned unsuccessful: %+v", sendResp)
	}

	histResp, err := client.GetRoomHistory(ctx, &pb.RoomHistoryRequest{
		RoomId: "engineering",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("GetRoomHistory failed: %v", err)
	}
	if len(histResp.Messages) == 0 {
		t.Fatalf("Expected at least 1 message in history, got 0")
	}
	if histResp.Messages[0].Content != "Hello engineering team!" {
		t.Fatalf("Unexpected content in history: %s", histResp.Messages[0].Content)
	}
}

func TestBidirectionalChatStream(t *testing.T) {
	client, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Establish stream for User 1 (Alice)
	stream1, err := client.ChatStream(ctx)
	if err != nil {
		t.Fatalf("Failed to open stream1: %v", err)
	}
	// Join handshake
	err = stream1.Send(&pb.ChatMessage{
		RoomId: "random",
		Sender: "Alice",
		Type:   pb.MessageType_JOIN,
	})
	if err != nil {
		t.Fatalf("stream1 join error: %v", err)
	}

	// Read initial join notice
	msg1, err := stream1.Recv()
	if err != nil {
		t.Fatalf("stream1 failed to receive initial msg: %v", err)
	}
	if msg1.Type != pb.MessageType_JOIN {
		t.Logf("Expected JOIN msg, got: %+v", msg1)
	}

	// 2. Establish stream for User 2 (Bob)
	stream2, err := client.ChatStream(ctx)
	if err != nil {
		t.Fatalf("Failed to open stream2: %v", err)
	}
	err = stream2.Send(&pb.ChatMessage{
		RoomId: "random",
		Sender: "Bob",
		Type:   pb.MessageType_JOIN,
	})
	if err != nil {
		t.Fatalf("stream2 join error: %v", err)
	}

	// Alice sends a message
	err = stream1.Send(&pb.ChatMessage{
		RoomId:  "random",
		Sender:  "Alice",
		Content: "Hey Bob!",
		Type:    pb.MessageType_TEXT,
	})
	if err != nil {
		t.Fatalf("Alice failed to send msg: %v", err)
	}

	// Bob should receive Alice's message
	receivedBob := false
	for i := 0; i < 3; i++ {
		msg, err := stream2.Recv()
		if err != nil {
			t.Fatalf("Bob recv error: %v", err)
		}
		if msg.Content == "Hey Bob!" && msg.Sender == "Alice" {
			receivedBob = true
			break
		}
	}

	if !receivedBob {
		t.Fatalf("Bob did not receive Alice's message")
	}

	_ = stream1.CloseSend()
	_ = stream2.CloseSend()
}

