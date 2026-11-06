package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("addr", "localhost:50051", "gRPC server address")
	roomID := flag.String("room", "", "Chat room ID (defaults to 'general')")
	username := flag.String("user", "", "User handle/display name")
	flag.Parse()

	reader := bufio.NewReader(os.Stdin)

	if *username == "" {
		fmt.Print("Enter your username: ")
		input, _ := reader.ReadString('\n')
		*username = strings.TrimSpace(input)
		if *username == "" {
			*username = fmt.Sprintf("user-%s", uuid.New().String()[:4])
		}
	}

	if *roomID == "" {
		fmt.Print("Enter room name [general]: ")
		input, _ := reader.ReadString('\n')
		*roomID = strings.TrimSpace(input)
		if *roomID == "" {
			*roomID = "general"
		}
	}

	fmt.Printf("\nConnecting to %s as '%s' in room '%s'...\n", *serverAddr, *username, *roomID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nDisconnecting...")
		cancel()
	}()

	conn, err := grpc.DialContext(ctx, *serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Could not connect to gRPC server at %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	chatClient := pb.NewChatServiceClient(conn)

	stream, err := chatClient.ChatStream(ctx)
	if err != nil {
		log.Fatalf("Error establishing chat stream: %v", err)
	}

	// 1. Send initial JOIN handshake
	joinMsg := &pb.ChatMessage{
		Id:          uuid.New().String(),
		RoomId:      *roomID,
		Sender:      *username,
		Content:     "",
		TimestampMs: time.Now().UnixMilli(),
		Type:        pb.MessageType_JOIN,
	}
	if err := stream.Send(joinMsg); err != nil {
		log.Fatalf("Failed to send join message: %v", err)
	}

	fmt.Println("Connected! Type messages and press Enter. Commands: /history, /quit")
	fmt.Println("------------------------------------------------------------------")

	// 2. Background receiver goroutine
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF || ctx.Err() != nil {
					return
				}
				fmt.Printf("\n[Disconnected from server: %v]\n", err)
				cancel()
				return
			}

			timestamp := time.UnixMilli(msg.TimestampMs).Format("15:04:05")

			switch msg.Type {
			case pb.MessageType_JOIN:
				fmt.Printf("\r[%s] >>> %s\n> ", timestamp, msg.Content)
			case pb.MessageType_LEAVE:
				fmt.Printf("\r[%s] <<< %s\n> ", timestamp, msg.Content)
			case pb.MessageType_MODERATION_FLAGGED:
				fmt.Printf("\r[%s] [SAFETY ALERT] %s\n> ", timestamp, msg.Content)
			default:
				if msg.Sender == *username {
					fmt.Printf("\r[%s] You: %s\n> ", timestamp, msg.Content)
				} else {
					fmt.Printf("\r[%s] %s: %s\n> ", timestamp, msg.Sender, msg.Content)
				}
			}
		}
	}()

	// 3. Sender loop
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			fmt.Print("> ")
			continue
		}

		if text == "/quit" || text == "/exit" {
			break
		}

		if text == "/history" {
			histResp, err := chatClient.GetRoomHistory(context.Background(), &pb.RoomHistoryRequest{
				RoomId: *roomID,
				Limit:  10,
			})
			if err != nil {
				fmt.Printf("Error fetching history: %v\n", err)
			} else {
				fmt.Println("--- Recent History ---")
				for _, h := range histResp.Messages {
					t := time.UnixMilli(h.TimestampMs).Format("15:04:05")
					fmt.Printf("[%s] %s: %s\n", t, h.Sender, h.Content)
				}
				fmt.Println("----------------------")
			}
			fmt.Print("> ")
			continue
		}

		sendErr := stream.Send(&pb.ChatMessage{
			Id:          uuid.New().String(),
			RoomId:      *roomID,
			Sender:      *username,
			Content:     text,
			TimestampMs: time.Now().UnixMilli(),
			Type:        pb.MessageType_TEXT,
		})

		if sendErr != nil {
			fmt.Printf("Failed to send: %v\n", sendErr)
			break
		}

		fmt.Print("> ")
	}

	_ = stream.CloseSend()
	fmt.Println("Goodbye!")
}

