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
	"time"
)

// GeminiClassifier implements LLMClassifier using Google Gemini Flash API.
type GeminiClassifier struct {
	apiKey string
	model  string
	client *http.Client
}

// Gemini REST API request/response structures
type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiRequest struct {
	Contents         []geminiContent  `json:"contents"`
	GenerationConfig geminiGenConfig  `json:"generationConfig"`
}

type geminiGenConfig struct {
	MaxOutputTokens int                   `json:"maxOutputTokens"`
	Temperature     float64               `json:"temperature"`
	ThinkingConfig  *geminiThinkingConfig `json:"thinkingConfig,omitempty"`
}

type geminiThinkingConfig struct {
	ThinkingBudget int `json:"thinkingBudget"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// NewGeminiClassifier creates a Gemini-backed content moderation classifier.
func NewGeminiClassifier(apiKey string, model string) *GeminiClassifier {
	if model == "" {
		model = "gemini-flash-latest"
	}
	return &GeminiClassifier{
		apiKey: apiKey,
		model:  model,
		client: &http.Client{Timeout: 25 * time.Second},
	}
}

// CheckSafety calls the Gemini API to classify message content.
func (g *GeminiClassifier) CheckSafety(ctx context.Context, text string) (bool, string, error) {
	prompt := fmt.Sprintf(
		"You are a strict chat content moderation filter.\n"+
			"Classify this message: \"%s\"\n"+
			"Rules:\n"+
			"- If it contains severe toxicity, hate speech, explicit threats of violence, or targeted harassment → reply strictly with: FLAGGED: <brief reason>\n"+
			"- Otherwise → reply strictly with: SAFE\n"+
			"Do not explain. Reply with one line only.",
		text,
	)

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{Parts: []geminiPart{{Text: prompt}}},
		},
		GenerationConfig: geminiGenConfig{
			MaxOutputTokens: 50,
			Temperature:     0.0,
			ThinkingConfig:  &geminiThinkingConfig{ThinkingBudget: 0},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return true, "", fmt.Errorf("failed to marshal gemini request: %w", err)
	}

	url := fmt.Sprintf(
		"https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s",
		g.model, g.apiKey,
	)

	var resp *http.Response
	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return true, "", fmt.Errorf("failed to create gemini http request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err = g.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// Retry on temporary 503 / 429
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("gemini returned temporary status %d", resp.StatusCode)
			time.Sleep(1 * time.Second)
			continue
		}

		lastErr = nil
		break
	}

	if lastErr != nil {
		log.Printf("[GeminiClassifier] Requests failed (fail-open): %v", lastErr)
		return true, "gemini_unavailable", lastErr
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return true, "", fmt.Errorf("failed to read gemini response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return true, "non_200_status", fmt.Errorf("gemini returned HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var genResp geminiResponse
	if err := json.Unmarshal(respBytes, &genResp); err != nil {
		return true, "", fmt.Errorf("failed to decode gemini response: %w", err)
	}

	if genResp.Error != nil {
		return true, "api_error", fmt.Errorf("gemini API error %d: %s", genResp.Error.Code, genResp.Error.Message)
	}

	if len(genResp.Candidates) == 0 || len(genResp.Candidates[0].Content.Parts) == 0 {
		return true, "empty_response", fmt.Errorf("gemini returned no candidates")
	}

	answer := strings.TrimSpace(genResp.Candidates[0].Content.Parts[0].Text)
	upper := strings.ToUpper(answer)

	log.Printf("[GeminiClassifier] Response for '%s': %s", text, answer)

	if strings.HasPrefix(upper, "FLAGGED") {
		return false, answer, nil
	}

	return true, "safe", nil
}
