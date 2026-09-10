package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/vegard/say/internal/random"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrInvalidToken     = errors.New("invalid worker token")
	ErrExpiredToken     = errors.New("expired worker token")
	ErrPlatformMismatch = errors.New("worker platform does not match account")
)

type Account struct {
	ID            string     `json:"id"`
	UserID        string     `json:"-"`
	Platform      string     `json:"platform"`
	ExternalID    string     `json:"external_id,omitempty"`
	DisplayName   string     `json:"display_name"`
	Status        string     `json:"status"`
	HostingMode   string     `json:"hosting_mode"`
	WorkerID      string     `json:"worker_id,omitempty"`
	Generation    uint64     `json:"generation"`
	LastSeenAt    *time.Time `json:"last_seen_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	EnrollmentEnd time.Time  `json:"-"`
}

type WorkerAuth struct {
	Account    Account
	Credential string
}

type AccountStore interface {
	CreateAccount(context.Context, string, string, string) (Account, string, error)
	ListAccounts(context.Context, string) ([]Account, error)
	GetAccount(context.Context, string, string) (Account, error)
	DeleteAccount(context.Context, string, string) error
	AuthenticateWorker(context.Context, string, string) (WorkerAuth, error)
	SetWorkerStatus(context.Context, string, uint64, string) error
	SetWorkerIdentity(context.Context, string, uint64, string, string) error
	TouchWorker(context.Context, string, uint64) error
}

type Store struct {
	mu          sync.RWMutex
	accounts    map[string]*Account
	enrollments map[[32]byte]string
	credentials map[[32]byte]string
}

func NewStore() *Store {
	return &Store{
		accounts:    make(map[string]*Account),
		enrollments: make(map[[32]byte]string),
		credentials: make(map[[32]byte]string),
	}
}

func (s *Store) CreateAccount(_ context.Context, userID, platform, displayName string) (Account, string, error) {
	if platform != "signal" && platform != "telegram" {
		return Account{}, "", errors.New("platform must be signal or telegram")
	}

	accountID, err := random.Value("acc_", 16)
	if err != nil {
		return Account{}, "", err
	}
	enrollment, err := random.Value("say_enroll_", 32)
	if err != nil {
		return Account{}, "", err
	}

	now := time.Now().UTC()
	account := &Account{
		ID:            accountID,
		UserID:        userID,
		Platform:      platform,
		DisplayName:   displayName,
		Status:        "unlinked",
		HostingMode:   "self_hosted",
		CreatedAt:     now,
		EnrollmentEnd: now.Add(10 * time.Minute),
	}

	s.mu.Lock()
	s.accounts[account.ID] = account
	s.enrollments[sha256.Sum256([]byte(enrollment))] = account.ID
	s.mu.Unlock()

	return *account, enrollment, nil
}

func (s *Store) ListAccounts(_ context.Context, userID string) ([]Account, error) {
	s.mu.RLock()
	accounts := make([]Account, 0)
	for _, account := range s.accounts {
		if account.UserID == userID {
			accounts = append(accounts, *account)
		}
	}
	s.mu.RUnlock()

	sort.Slice(accounts, func(i, j int) bool {
		return accounts[i].CreatedAt.Before(accounts[j].CreatedAt)
	})
	return accounts, nil
}

func (s *Store) GetAccount(_ context.Context, userID, accountID string) (Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	account, ok := s.accounts[accountID]
	if !ok || account.UserID != userID {
		return Account{}, ErrNotFound
	}
	return *account, nil
}

func (s *Store) DeleteAccount(_ context.Context, userID, accountID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	account, ok := s.accounts[accountID]
	if !ok || account.UserID != userID {
		return ErrNotFound
	}
	delete(s.accounts, accountID)
	for tokenHash, id := range s.enrollments {
		if id == accountID {
			delete(s.enrollments, tokenHash)
		}
	}
	for tokenHash, id := range s.credentials {
		if id == accountID {
			delete(s.credentials, tokenHash)
		}
	}
	return nil
}

func (s *Store) AuthenticateWorker(_ context.Context, token, platform string) (WorkerAuth, error) {
	tokenHash := sha256.Sum256([]byte(token))

	s.mu.Lock()
	defer s.mu.Unlock()

	if accountID, ok := s.credentials[tokenHash]; ok {
		account, exists := s.accounts[accountID]
		if !exists {
			return WorkerAuth{}, ErrInvalidToken
		}
		if account.Platform != platform {
			return WorkerAuth{}, ErrPlatformMismatch
		}
		return WorkerAuth{Account: *account}, nil
	}

	accountID, ok := s.enrollments[tokenHash]
	if !ok {
		return WorkerAuth{}, ErrInvalidToken
	}
	account, exists := s.accounts[accountID]
	if !exists {
		delete(s.enrollments, tokenHash)
		return WorkerAuth{}, ErrInvalidToken
	}
	if time.Now().After(account.EnrollmentEnd) {
		delete(s.enrollments, tokenHash)
		return WorkerAuth{}, ErrExpiredToken
	}
	if account.Platform != platform {
		return WorkerAuth{}, ErrPlatformMismatch
	}

	credential, err := random.Value("say_worker_", 32)
	if err != nil {
		return WorkerAuth{}, err
	}
	workerID, err := random.Value("wrk_", 16)
	if err != nil {
		return WorkerAuth{}, err
	}

	delete(s.enrollments, tokenHash)
	s.credentials[sha256.Sum256([]byte(credential))] = account.ID
	account.WorkerID = workerID
	account.Generation++
	account.Status = "offline"
	account.EnrollmentEnd = time.Time{}

	return WorkerAuth{Account: *account, Credential: credential}, nil
}

func (s *Store) SetWorkerStatus(_ context.Context, accountID string, generation uint64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	account, ok := s.accounts[accountID]
	if !ok || account.Generation != generation {
		return nil
	}
	account.Status = status
	now := time.Now().UTC()
	account.LastSeenAt = &now
	return nil
}

func (s *Store) TouchWorker(_ context.Context, accountID string, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	account, ok := s.accounts[accountID]
	if !ok || account.Generation != generation {
		return nil
	}
	now := time.Now().UTC()
	account.LastSeenAt = &now
	return nil
}

func (s *Store) SetWorkerIdentity(
	_ context.Context,
	accountID string,
	generation uint64,
	status, externalID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	account, ok := s.accounts[accountID]
	if !ok || account.Generation != generation {
		return nil
	}
	account.Status = status
	account.ExternalID = externalID
	now := time.Now().UTC()
	account.LastSeenAt = &now
	return nil
}
