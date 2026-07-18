package webhook

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type EventType string

const (
	EventMatchDetected      EventType = "MATCH_DETECTED"
	EventDerivativeDetected EventType = "DERIVATIVE_DETECTED"
	EventPlagiarismAlert    EventType = "PLAGIARISM_ALERT"
)

type Payload struct {
	EventType    EventType `json:"event_type"`
	OriginalHash string    `json:"original_hash"`
	Similarity   float64   `json:"similarity"`
	Timestamp    string    `json:"timestamp"`
	Message      string    `json:"message"`

	Content string `json:"content,omitempty"`
	Text    string `json:"text,omitempty"`
}

type Dispatcher interface {
	Dispatch(url string, payload Payload)
}

type dispatcher struct {
	client *http.Client
}

func NewDispatcher() Dispatcher {
	return &dispatcher{
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (d *dispatcher) Dispatch(url string, payload Payload) {
	if url == "" {
		return
	}

	payload.Content = payload.Message
	payload.Text = payload.Message

	go func() {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("[Webhook] Failed to marshal payload: %v\n", err)
			return
		}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadBytes))
		if err != nil {
			log.Printf("[Webhook] Failed to create request: %v\n", err)
			return
		}

		req.Header.Set("Content-Type", "application/json")

		log.Printf("[Webhook] Dispatching event %s to %s\n", payload.EventType, url)
		resp, err := d.client.Do(req)
		if err != nil {
			log.Printf("[Webhook] Failed to dispatch webhook to %s: %v\n", url, err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("[Webhook] Successfully dispatched event %s. Status: %d\n", payload.EventType, resp.StatusCode)
		} else {
			log.Printf("[Webhook] Received non-200 response from webhook %s: %d\n", url, resp.StatusCode)
		}
	}()
}
