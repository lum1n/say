package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// EmailServiceSender delivers login codes through email-service's
// POST /emails/send endpoint.
type EmailServiceSender struct {
	URL    string
	From   string
	Token  string
	Client *http.Client
}

func NewEmailServiceSender(url, from, token string) (EmailServiceSender, error) {
	if strings.TrimSpace(url) == "" {
		return EmailServiceSender{}, errors.New("email-service URL is required")
	}
	if strings.TrimSpace(from) == "" {
		return EmailServiceSender{}, errors.New("login email From address is required")
	}
	return EmailServiceSender{
		URL:   strings.TrimRight(strings.TrimSpace(url), "/"),
		From:  strings.TrimSpace(from),
		Token: strings.TrimSpace(token),
	}, nil
}

func (s EmailServiceSender) SendLoginCode(ctx context.Context, email, code string) error {
	body, err := json.Marshal(map[string]any{
		"from":    s.From,
		"to":      email,
		"subject": "Your say login code",
		"text":    fmt.Sprintf("Your say login code is %s.\n\nIt expires in 10 minutes.", code),
		"html":    fmt.Sprintf("<p>Your say login code is <strong>%s</strong>.</p><p>It expires in 10 minutes.</p>", code),
		"tags": []map[string]string{
			{"name": "type", "value": "login_code"},
			{"name": "service", "value": "say"},
		},
		"metadata": map[string]string{
			"purpose": "login_code",
			"service": "say",
		},
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create delivery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Service-Name", "say")
	if s.Token != "" {
		request.Header.Set("X-Service-Key", s.Token)
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
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail := strings.TrimSpace(string(responseBody))
		if detail == "" {
			return fmt.Errorf("delivery endpoint returned HTTP %d", response.StatusCode)
		}
		return fmt.Errorf("delivery endpoint returned HTTP %d: %s", response.StatusCode, detail)
	}
	return nil
}
