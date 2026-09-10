package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type WebhookSender struct {
	URL    string
	Token  string
	Client *http.Client
}

func (s WebhookSender) SendLoginCode(ctx context.Context, email, code string) error {
	body, err := json.Marshal(map[string]string{
		"email": email,
		"code":  code,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create delivery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if s.Token != "" {
		request.Header.Set("Authorization", "Bearer "+s.Token)
	}
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("deliver login code: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("delivery endpoint returned HTTP %d", response.StatusCode)
	}
	return nil
}
