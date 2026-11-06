package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sagar/grpc-chat-system/pkg/chat"
	"github.com/sagar/grpc-chat-system/pkg/moderation"
	"github.com/sagar/grpc-chat-system/pkg/pb"
	"github.com/sagar/grpc-chat-system/pkg/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := flag.Int("port", 50051, "gRPC server port")
	redisAddr := flag.String("redis-addr", getEnv("REDIS_ADDR", "localhost:6379"), "Redis address for Pub/Sub")
	redisPass := flag.String("redis-pass", getEnv("REDIS_PASS", ""), "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB index")
	useMemoryBroker := flag.Bool("use-memory-broker", getEnv("USE_MEMORY_BROKER", "false") == "true", "Force in-memory message broker instead of Redis")
	ollamaURL := flag.String("ollama-url", getEnv("OLLAMA_URL", "http://localhost:11434"), "Ollama API base URL")
	ollamaModel := flag.String("ollama-model", getEnv("OLLAMA_MODEL", "llama3.2:1b"), "Ollama model name (e.g. llama3.2:1b or phi3:mini)")
	geminiAPIKey := flag.String("gemini-api-key", getEnv("GEMINI_API_KEY", ""), "Google Gemini API key (overrides Ollama when set)")
	geminiModel := flag.String("gemini-model", getEnv("GEMINI_MODEL", "gemini-flash-latest"), "Gemini model name")
	enableModeration := flag.Bool("enable-moderation", getEnv("ENABLE_MODERATION", "true") == "true", "Enable async LLM content moderation")
	backpressurePolicyStr := flag.String("backpressure-policy", getEnv("BACKPRESSURE_POLICY", "drop_oldest"), "Backpressure policy: drop_oldest, disconnect, drop_newest")
	bufferSize := flag.Int("buffer-size", 64, "Per-client outbound channel buffer size")

	flag.Parse()

	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			*port = p
		}
	}

	serverID := fmt.Sprintf("pod-%s", uuid.New().String()[:6])
	log.Printf("=====================================================================")
	log.Printf("Starting Real-Time gRPC Chat Server [%s]", serverID)
	log.Printf("Port: %d | Policy: %s | Buffer: %d", *port, *backpressurePolicyStr, *bufferSize)
	log.Printf("=====================================================================")

	// 1. Initialize Message Broker (Redis Pub/Sub or In-Memory fallback)
	var broker redis.MessageBroker
	if *useMemoryBroker {
		log.Println("[Init] Using in-memory message broker (isolated pod mode).")
		broker = redis.NewMemoryBroker()
	} else {
		log.Printf("[Init] Connecting to Redis Pub/Sub at %s...", *redisAddr)
		rb, err := redis.NewRedisBroker(*redisAddr, *redisPass, *redisDB)
		if err != nil {
			log.Printf("[Init] WARNING: Could not connect to Redis (%v). Falling back to in-memory broker.", err)
			broker = redis.NewMemoryBroker()
		} else {
			broker = rb
		}
	}

	// 2. Initialize Hub and Moderation Pipeline
	var moderationPipe *moderation.AsyncModerationPipeline
	var hub *chat.Hub

	if *enableModeration {
		// Auto-select classifier: Gemini if API key provided, else Ollama
		var classifier moderation.LLMClassifier
		if *geminiAPIKey != "" {
			log.Printf("[Init] Enabling async Gemini moderation with model '%s'", *geminiModel)
			classifier = moderation.NewGeminiClassifier(*geminiAPIKey, *geminiModel)
		} else {
			log.Printf("[Init] Enabling async Ollama moderation with model '%s' via %s", *ollamaModel, *ollamaURL)
			classifier = moderation.NewOllamaClassifier(*ollamaURL, *ollamaModel)
		}

		moderationPipe = moderation.NewAsyncModerationPipeline(
			classifier,
			500, // queue size
			4,   // workers
			func(originalMsg *pb.ChatMessage, reason string) {
				log.Printf("[MODERATION EVENT] Message ID %s by '%s' in room '%s' was FLAGGED. Reason: %s",
					originalMsg.Id, originalMsg.Sender, originalMsg.RoomId, reason)

				if hub != nil {
					flaggedNotice := &pb.ChatMessage{
						Id:          uuid.New().String(),
						RoomId:      originalMsg.RoomId,
						Sender:      "AI-MODERATOR",
						Content:     fmt.Sprintf("[RETRACTED] Message from '%s' was flagged and hidden due to safety violation.", originalMsg.Sender),
						TimestampMs: time.Now().UnixMilli(),
						Type:        pb.MessageType_MODERATION_FLAGGED,
						Metadata: map[string]string{
							"flagged_message_id": originalMsg.Id,
							"reason":             reason,
						},
					}
					_ = hub.Publish(context.Background(), flaggedNotice)
				}
			},
		)
	}

	// 3. Initialize Hub
	hub = chat.NewHub(serverID, broker, moderationPipe)

	// 4. Initialize gRPC Server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}

	var policy chat.BackpressurePolicy
	switch *backpressurePolicyStr {
	case "disconnect":
		policy = chat.PolicyDisconnect
	case "drop_newest":
		policy = chat.PolicyDropNewest
	default:
		policy = chat.PolicyDropOldest
	}

	chatServer := chat.NewServer(hub, *bufferSize, policy)

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Minute,
			Timeout:               1 * time.Second,
		}),
	)

	pb.RegisterChatServiceServer(grpcServer, chatServer)
	reflection.Register(grpcServer)

	// 5. Graceful Shutdown Setup
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stopChan
		log.Println("[Shutdown] Initiating graceful shutdown...")

		// Stop accepting new RPCs and drain connections
		grpcServer.GracefulStop()

		// Close hub and broker
		_ = hub.Close()

		// Stop moderation workers
		if moderationPipe != nil {
			moderationPipe.Close()
		}

		log.Println("[Shutdown] Server stopped cleanly.")
		os.Exit(0)
	}()

	log.Printf("[Server] gRPC Chat Server listening at %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server error: %v", err)
	}
}
