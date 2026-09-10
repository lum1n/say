package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/random"
)

type AccountStore struct {
	pool *pgxpool.Pool
}

func NewAccountStore(pool *pgxpool.Pool) *AccountStore {
	return &AccountStore{pool: pool}
}

func (s *AccountStore) MarkWorkersOffline(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE accounts
		SET status = 'offline'
		WHERE status = 'live'
	`)
	if err != nil {
		return fmt.Errorf("mark workers offline: %w", err)
	}
	return nil
}

func (s *AccountStore) CreateAccount(
	ctx context.Context,
	userID, platform, displayName string,
) (gateway.Account, string, error) {
	if platform != "signal" && platform != "telegram" {
		return gateway.Account{}, "", errors.New("platform must be signal or telegram")
	}
	accountID, err := random.Value("acc_", 16)
	if err != nil {
		return gateway.Account{}, "", err
	}
	enrollmentID, err := random.Value("enr_", 16)
	if err != nil {
		return gateway.Account{}, "", err
	}
	enrollment, err := random.Value("say_enroll_", 32)
	if err != nil {
		return gateway.Account{}, "", err
	}
	expiresAt := time.Now().UTC().Add(10 * time.Minute)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return gateway.Account{}, "", fmt.Errorf("begin create account: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	account := gateway.Account{
		ID:            accountID,
		UserID:        userID,
		Platform:      platform,
		DisplayName:   displayName,
		Status:        "unlinked",
		HostingMode:   "self_hosted",
		EnrollmentEnd: expiresAt,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO accounts (id, user_id, platform, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at
	`, account.ID, account.UserID, account.Platform, account.DisplayName).Scan(&account.CreatedAt)
	if err != nil {
		return gateway.Account{}, "", fmt.Errorf("insert account: %w", err)
	}
	tokenHash := sha256.Sum256([]byte(enrollment))
	if _, err := tx.Exec(ctx, `
		INSERT INTO pairing_tokens (id, account_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4)
	`, enrollmentID, account.ID, tokenHash[:], expiresAt); err != nil {
		return gateway.Account{}, "", fmt.Errorf("insert pairing token: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return gateway.Account{}, "", fmt.Errorf("commit create account: %w", err)
	}
	return account, enrollment, nil
}

func (s *AccountStore) ListAccounts(ctx context.Context, userID string) ([]gateway.Account, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, user_id, platform, COALESCE(external_id, ''), display_name, status, hosting_mode,
		       COALESCE(worker_id, ''), generation, last_seen_at, created_at
		FROM accounts
		WHERE user_id = $1
		ORDER BY created_at
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()

	accounts := make([]gateway.Account, 0)
	for rows.Next() {
		account, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("scan account: %w", err)
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list account rows: %w", err)
	}
	return accounts, nil
}

func (s *AccountStore) GetAccount(
	ctx context.Context,
	userID, accountID string,
) (gateway.Account, error) {
	account, err := scanAccount(s.pool.QueryRow(ctx, `
		SELECT id, user_id, platform, COALESCE(external_id, ''), display_name, status, hosting_mode,
		       COALESCE(worker_id, ''), generation, last_seen_at, created_at
		FROM accounts
		WHERE id = $1 AND user_id = $2
	`, accountID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return gateway.Account{}, gateway.ErrNotFound
	}
	if err != nil {
		return gateway.Account{}, fmt.Errorf("get account: %w", err)
	}
	return account, nil
}

func (s *AccountStore) DeleteAccount(ctx context.Context, userID, accountID string) error {
	tag, err := s.pool.Exec(ctx, "DELETE FROM accounts WHERE id = $1 AND user_id = $2", accountID, userID)
	if err != nil {
		return fmt.Errorf("delete account: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return gateway.ErrNotFound
	}
	return nil
}

func (s *AccountStore) AuthenticateWorker(
	ctx context.Context,
	token, platform string,
) (gateway.WorkerAuth, error) {
	tokenHash := sha256.Sum256([]byte(token))
	account, err := scanAccount(s.pool.QueryRow(ctx, `
		SELECT a.id, a.user_id, a.platform, COALESCE(a.external_id, ''),
		       a.display_name, a.status, a.hosting_mode,
		       COALESCE(a.worker_id, ''), a.generation, a.last_seen_at, a.created_at
		FROM worker_credentials w
		JOIN accounts a ON a.id = w.account_id
		WHERE w.token_hash = $1 AND w.revoked_at IS NULL
	`, tokenHash[:]))
	if err == nil {
		if account.Platform != platform {
			return gateway.WorkerAuth{}, gateway.ErrPlatformMismatch
		}
		return gateway.WorkerAuth{Account: account}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return gateway.WorkerAuth{}, fmt.Errorf("lookup worker credential: %w", err)
	}
	return s.redeemEnrollment(ctx, tokenHash, platform)
}

func (s *AccountStore) redeemEnrollment(
	ctx context.Context,
	tokenHash [32]byte,
	platform string,
) (gateway.WorkerAuth, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("begin enrollment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var pairingID string
	var expiresAt time.Time
	var redeemedAt, revokedAt *time.Time
	account, err := scanEnrollment(tx.QueryRow(ctx, `
		SELECT p.id, p.expires_at, p.redeemed_at, p.revoked_at,
		       a.id, a.user_id, a.platform, COALESCE(a.external_id, ''),
		       a.display_name, a.status, a.hosting_mode,
		       COALESCE(a.worker_id, ''), a.generation, a.last_seen_at, a.created_at
		FROM pairing_tokens p
		JOIN accounts a ON a.id = p.account_id
		WHERE p.token_hash = $1
		FOR UPDATE OF p, a
	`, tokenHash[:]), &pairingID, &expiresAt, &redeemedAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return gateway.WorkerAuth{}, gateway.ErrInvalidToken
	}
	if err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("read enrollment: %w", err)
	}
	if redeemedAt != nil || revokedAt != nil {
		return gateway.WorkerAuth{}, gateway.ErrInvalidToken
	}
	if time.Now().After(expiresAt) {
		return gateway.WorkerAuth{}, gateway.ErrExpiredToken
	}
	if account.Platform != platform {
		return gateway.WorkerAuth{}, gateway.ErrPlatformMismatch
	}

	credential, err := random.Value("say_worker_", 32)
	if err != nil {
		return gateway.WorkerAuth{}, err
	}
	workerID, err := random.Value("wrk_", 16)
	if err != nil {
		return gateway.WorkerAuth{}, err
	}
	credentialID, err := random.Value("wcr_", 16)
	if err != nil {
		return gateway.WorkerAuth{}, err
	}
	credentialHash := sha256.Sum256([]byte(credential))

	if _, err := tx.Exec(ctx,
		"UPDATE pairing_tokens SET redeemed_at = now() WHERE id = $1",
		pairingID,
	); err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("redeem pairing token: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO worker_credentials (id, account_id, token_hash)
		VALUES ($1, $2, $3)
	`, credentialID, account.ID, credentialHash[:]); err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("insert worker credential: %w", err)
	}
	var generation int64
	err = tx.QueryRow(ctx, `
		UPDATE accounts
		SET worker_id = $2, generation = generation + 1, status = 'offline'
		WHERE id = $1
		RETURNING generation
	`, account.ID, workerID).Scan(&generation)
	if err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("activate worker: %w", err)
	}
	account.Generation = uint64(generation)
	account.WorkerID = workerID
	account.Status = "offline"

	if err := tx.Commit(ctx); err != nil {
		return gateway.WorkerAuth{}, fmt.Errorf("commit enrollment: %w", err)
	}
	return gateway.WorkerAuth{Account: account, Credential: credential}, nil
}

func (s *AccountStore) SetWorkerStatus(
	ctx context.Context,
	accountID string,
	generation uint64,
	status string,
) error {
	if status != "live" && status != "offline" && status != "unlinked" &&
		status != "linking" && status != "upgrade_required" {
		return fmt.Errorf("invalid worker status %q", status)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE accounts
		SET status = $3, last_seen_at = now()
		WHERE id = $1 AND generation = $2
	`, accountID, generation, status)
	if err != nil {
		return fmt.Errorf("set worker status: %w", err)
	}
	return nil
}

func (s *AccountStore) TouchWorker(
	ctx context.Context,
	accountID string,
	generation uint64,
) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE accounts
		SET last_seen_at = now()
		WHERE id = $1 AND generation = $2
	`, accountID, generation)
	if err != nil {
		return fmt.Errorf("touch worker: %w", err)
	}
	return nil
}

func (s *AccountStore) SetWorkerIdentity(
	ctx context.Context,
	accountID string,
	generation uint64,
	status, externalID string,
) error {
	if status != "live" && status != "unlinked" && status != "linking" &&
		status != "upgrade_required" {
		return fmt.Errorf("invalid worker identity status %q", status)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE accounts
		SET status = $3, external_id = NULLIF($4, ''), last_seen_at = now(),
		    linked_at = CASE WHEN $3 = 'live' THEN COALESCE(linked_at, now()) ELSE linked_at END
		WHERE id = $1 AND generation = $2
	`, accountID, generation, status, externalID)
	if err != nil {
		return fmt.Errorf("set worker identity: %w", err)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAccount(row scanner) (gateway.Account, error) {
	var account gateway.Account
	var generation int64
	err := row.Scan(
		&account.ID,
		&account.UserID,
		&account.Platform,
		&account.ExternalID,
		&account.DisplayName,
		&account.Status,
		&account.HostingMode,
		&account.WorkerID,
		&generation,
		&account.LastSeenAt,
		&account.CreatedAt,
	)
	account.Generation = uint64(generation)
	return account, err
}

func scanEnrollment(
	row scanner,
	pairingID *string,
	expiresAt *time.Time,
	redeemedAt, revokedAt **time.Time,
) (gateway.Account, error) {
	var account gateway.Account
	var generation int64
	err := row.Scan(
		pairingID,
		expiresAt,
		redeemedAt,
		revokedAt,
		&account.ID,
		&account.UserID,
		&account.Platform,
		&account.ExternalID,
		&account.DisplayName,
		&account.Status,
		&account.HostingMode,
		&account.WorkerID,
		&generation,
		&account.LastSeenAt,
		&account.CreatedAt,
	)
	account.Generation = uint64(generation)
	return account, err
}
