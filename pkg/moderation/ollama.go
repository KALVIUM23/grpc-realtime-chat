package moderation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sagar/grpc-chat-system/pkg/pb"
)

// LLMClassifier defines the interface for content moderation.
type LLMClassifier interface {
	CheckSafety(ctx context.Context, text string) (isSafe bool, reason string, err error)
}

// OllamaClassifier implements LLMClassifier by calling a local Ollama server.
type OllamaClassifier struct {
	baseURL string
	model   string
	client  *http.Client
}

// OllamaGenerateRequest is the JSON payload for Ollama's /api/generate endpoint.
type OllamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

// OllamaGenerateResponse is the JSON response from Ollama's /api/generate endpoint.
type OllamaGenerateResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

// NewOllamaClassifier initializes an Ollama client.
func NewOllamaClassifier(baseURL, model string) *OllamaClassifier {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "llama3.2:1b" // Fast lightweight model default
	}

	return &OllamaClassifier{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// CheckSafety queries the local Ollama LLM to classify chat content.
func (o *OllamaClassifier) CheckSafety(ctx context.Context, text string) (bool, string, error) {
	prompt := fmt.Sprintf(
		"You are a strict real-time chat content moderation filter. Classify the following message: \n\"%s\"\n"+
			"Rules: If it contains severe toxicity, hate speech, threats of violence, or explicit harassment, reply with 'FLAGGED: <brief reason>'. "+
			"Otherwise, reply strictly with 'SAFE'. Do not provide conversational filler.",
		text,
	)

	reqPayload := OllamaGenerateRequest{
		Model:  o.model,
		Prompt: prompt,
		Stream: false,
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return true, "", fmt.Errorf("failed to marshal ollama request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/api/generate", bytes.NewReader(bodyBytes))
	if err != nil {
		return true, "", fmt.Errorf("failed to create ollama http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		// Log warning and fail-open for chat continuity if Ollama is temporarily unavailable
		return true, "ollama_unreachable", fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return true, "non_200_status", fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var genResp OllamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&genResp); err != nil {
		return true, "", fmt.Errorf("failed to decode ollama response: %w", err)
	}

	trimmed := strings.TrimSpace(genResp.Response)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "FLAGGED") {
		return false, trimmed, nil
	}

	return true, "safe", nil
}

// AsyncModerationPipeline manages background queueing and moderation.
type AsyncModerationPipeline struct {
	classifier     LLMClassifier
	queue          chan *pb.ChatMessage
	onFlagged      func(originalMsg *pb.ChatMessage, reason string)
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	workers        int
	processedCount int64
	flaggedCount   int64
	mu             sync.Mutex
}

// NewAsyncModerationPipeline initializes a pool of background workers for moderation.
func NewAsyncModerationPipeline(classifier LLMClassifier, queueSize int, workers int, onFlagged func(originalMsg *pb.ChatMessage, reason string)) *AsyncModerationPipeline {
	if queueSize <= 0 {
		queueSize = 500
	}
	if workers <= 0 {
		workers = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	pipe := &AsyncModerationPipeline{
		classifier: classifier,
		queue:      make(chan *pb.ChatMessage, queueSize),
		onFlagged:  onFlagged,
		ctx:        ctx,
		cancel:     cancel,
		workers:    workers,
	}

	pipe.startWorkers()
	return pipe
}

func (p *AsyncModerationPipeline) startWorkers() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case msg, ok := <-p.queue:
					if !ok {
						return
					}

					// Process message
					checkCtx, checkCancel := context.WithTimeout(p.ctx, 30*time.Second)
					isSafe, reason, err := p.classifier.CheckSafety(checkCtx, msg.Content)
					checkCancel()

					p.mu.Lock()
					p.processedCount++
					if err != nil {
						log.Printf("[ModeratorWorker-%d] Moderation error for message %s: %v", workerID, msg.Id, err)
					} else if !isSafe {
						p.flaggedCount++
						log.Printf("[ModeratorWorker-%d] VIOLATION DETECTED for msg %s from %s: %s", workerID, msg.Id, msg.Sender, reason)
						if p.onFlagged != nil {
							p.onFlagged(msg, reason)
						}
					}
					p.mu.Unlock()
				}
			}
		}(i)
	}
}

// Enqueue adds a message to the async moderation queue without blocking chat.
func (p *AsyncModerationPipeline) Enqueue(msg *pb.ChatMessage) {
	// Only inspect standard user text messages
	if msg.Type != pb.MessageType_TEXT || strings.TrimSpace(msg.Content) == "" {
		return
	}

	select {
	case p.queue <- msg:
		// Enqueued successfully
	default:
		// Queue full: log backpressure drop on moderation queue so chat message flow is never blocked
		log.Printf("[ModerationPipeline] Moderation queue full; skipping async inspection for msg %s", msg.Id)
	}
}

// GetStats returns processed and flagged counters.
func (p *AsyncModerationPipeline) GetStats() (processed int64, flagged int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processedCount, p.flaggedCount
}

// Close gracefully stops the moderation workers.
func (p *AsyncModerationPipeline) Close() {
	p.cancel()
	p.wg.Wait()
}

