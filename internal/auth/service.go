package auth

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"net/mail"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vegard/say/internal/random"
)

var (
	ErrInvalidLogin        = errors.New("invalid or expired login")
	ErrInvalidRefresh      = errors.New("invalid or expired refresh token")
	ErrInvalidAccess       = errors.New("invalid access token")
	ErrDeliveryUnavailable = errors.New("login code delivery is not configured")
	ErrInvalidEmail        = errors.New("invalid email address")
)

type CodeSender interface {
	SendLoginCode(context.Context, string, string) error
}

type Config struct {
	Issuer          string
	JWTSigningKey   []byte
	LoginHashKey    []byte
	AccessLifetime  time.Duration
	RefreshLifetime time.Duration
	LoginLifetime   time.Duration
	DebugReturnCode bool
}

type Service struct {
	pool   *pgxpool.Pool
	cfg    Config
	sender CodeSender
}

type LoginStart struct {
	LoginID   string    `json:"login_id"`
	ExpiresAt time.Time `json:"expires_at"`
	DebugCode string    `json:"debug_code,omitempty"`
}

type TokenPair struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type Claims struct {
	UserID    string `json:"uid"`
	TokenType string `json:"typ"`
	jwt.RegisteredClaims
}

func NewService(pool *pgxpool.Pool, cfg Config, sender CodeSender) (*Service, error) {
	if len(cfg.JWTSigningKey) < 32 {
		return nil, errors.New("JWT signing key must contain at least 32 bytes")
	}
	if len(cfg.LoginHashKey) < 32 {
		return nil, errors.New("login hash key must contain at least 32 bytes")
	}
	if cfg.Issuer == "" {
		cfg.Issuer = "say"
	}
	if cfg.AccessLifetime == 0 {
		cfg.AccessLifetime = 15 * time.Minute
	}
	if cfg.RefreshLifetime == 0 {
		cfg.RefreshLifetime = 30 * 24 * time.Hour
	}
	if cfg.LoginLifetime == 0 {
		cfg.LoginLifetime = 10 * time.Minute
	}
	return &Service{pool: pool, cfg: cfg, sender: sender}, nil
}

func (s *Service) Start(ctx context.Context, rawEmail string) (LoginStart, error) {
	if s.sender == nil && !s.cfg.DebugReturnCode {
		return LoginStart{}, ErrDeliveryUnavailable
	}
	email, err := normalizeEmail(rawEmail)
	if err != nil {
		return LoginStart{}, err
	}
	loginID, err := random.Value("login_", 16)
	if err != nil {
		return LoginStart{}, err
	}
	code, err := loginCode()
	if err != nil {
		return LoginStart{}, err
	}
	expiresAt := time.Now().UTC().Add(s.cfg.LoginLifetime)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO auth_logins (id, email, code_hash, expires_at)
		VALUES ($1, $2, $3, $4)
	`, loginID, email, s.hashLoginCode(loginID, code), expiresAt); err != nil {
		return LoginStart{}, fmt.Errorf("store login: %w", err)
	}
	if s.sender != nil {
		if err := s.sender.SendLoginCode(ctx, email, code); err != nil {
			return LoginStart{}, fmt.Errorf("send login code: %w", err)
		}
	}
	result := LoginStart{LoginID: loginID, ExpiresAt: expiresAt}
	if s.cfg.DebugReturnCode {
		result.DebugCode = code
	}
	return result, nil
}

func (s *Service) Verify(ctx context.Context, loginID, code string) (TokenPair, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TokenPair{}, fmt.Errorf("begin login verification: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var email string
	var expectedHash []byte
	var expiresAt time.Time
	var attempts int
	var consumedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT email, code_hash, expires_at, attempts, consumed_at
		FROM auth_logins
		WHERE id = $1
		FOR UPDATE
	`, loginID).Scan(&email, &expectedHash, &expiresAt, &attempts, &consumedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidLogin
	}
	if err != nil {
		return TokenPair{}, fmt.Errorf("read login: %w", err)
	}
	if consumedAt != nil || attempts >= 5 || time.Now().After(expiresAt) {
		return TokenPair{}, ErrInvalidLogin
	}

	actualHash := s.hashLoginCode(loginID, code)
	if !hmac.Equal(expectedHash, actualHash) {
		_, updateErr := tx.Exec(ctx, `
			UPDATE auth_logins
			SET attempts = attempts + 1,
			    consumed_at = CASE WHEN attempts + 1 >= 5 THEN now() ELSE consumed_at END
			WHERE id = $1
		`, loginID)
		if updateErr != nil {
			return TokenPair{}, fmt.Errorf("record failed login: %w", updateErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return TokenPair{}, fmt.Errorf("commit failed login: %w", err)
		}
		return TokenPair{}, ErrInvalidLogin
	}

	if _, err := tx.Exec(ctx, "UPDATE auth_logins SET consumed_at = now() WHERE id = $1", loginID); err != nil {
		return TokenPair{}, fmt.Errorf("consume login: %w", err)
	}
	proposedUserID, err := random.Value("usr_", 16)
	if err != nil {
		return TokenPair{}, err
	}
	var userID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO users (id, email)
		VALUES ($1, $2)
		ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
		RETURNING id
	`, proposedUserID, email).Scan(&userID); err != nil {
		return TokenPair{}, fmt.Errorf("upsert user: %w", err)
	}

	pair, err := s.issueTokenPair(ctx, tx, userID)
	if err != nil {
		return TokenPair{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, fmt.Errorf("commit login: %w", err)
	}
	return pair, nil
}

func (s *Service) Refresh(ctx context.Context, refreshToken string) (TokenPair, error) {
	tokenHash := sha256.Sum256([]byte(refreshToken))
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TokenPair{}, fmt.Errorf("begin refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tokenID, userID string
	var expiresAt time.Time
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, user_id, expires_at, revoked_at
		FROM refresh_tokens
		WHERE token_hash = $1
		FOR UPDATE
	`, tokenHash[:]).Scan(&tokenID, &userID, &expiresAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidRefresh
	}
	if err != nil {
		return TokenPair{}, fmt.Errorf("read refresh token: %w", err)
	}
	if revokedAt != nil || time.Now().After(expiresAt) {
		return TokenPair{}, ErrInvalidRefresh
	}

	pair, replacementID, err := s.issueTokenPairWithID(ctx, tx, userID)
	if err != nil {
		return TokenPair{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE refresh_tokens
		SET revoked_at = now(), replaced_by = $2
		WHERE id = $1
	`, tokenID, replacementID); err != nil {
		return TokenPair{}, fmt.Errorf("revoke refresh token: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, fmt.Errorf("commit refresh: %w", err)
	}
	return pair, nil
}

func (s *Service) ValidateAccessToken(rawToken string) (string, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidAccess
		}
		return s.cfg.JWTSigningKey, nil
	}, jwt.WithIssuer(s.cfg.Issuer), jwt.WithExpirationRequired())
	if err != nil || !token.Valid || claims.TokenType != "access" || claims.UserID == "" {
		return "", ErrInvalidAccess
	}
	return claims.UserID, nil
}

func (s *Service) issueTokenPair(ctx context.Context, tx pgx.Tx, userID string) (TokenPair, error) {
	pair, _, err := s.issueTokenPairWithID(ctx, tx, userID)
	return pair, err
}

func (s *Service) issueTokenPairWithID(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
) (TokenPair, string, error) {
	now := time.Now().UTC()
	accessExpiresAt := now.Add(s.cfg.AccessLifetime)
	claims := Claims{
		UserID:    userID,
		TokenType: "access",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(accessExpiresAt),
		},
	}
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.cfg.JWTSigningKey)
	if err != nil {
		return TokenPair{}, "", fmt.Errorf("sign access token: %w", err)
	}
	refreshToken, err := random.Value("say_refresh_", 32)
	if err != nil {
		return TokenPair{}, "", err
	}
	refreshID, err := random.Value("rft_", 16)
	if err != nil {
		return TokenPair{}, "", err
	}
	refreshHash := sha256.Sum256([]byte(refreshToken))
	if _, err := tx.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4)
	`, refreshID, userID, refreshHash[:], now.Add(s.cfg.RefreshLifetime)); err != nil {
		return TokenPair{}, "", fmt.Errorf("store refresh token: %w", err)
	}
	return TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresAt:    accessExpiresAt,
	}, refreshID, nil
}

func (s *Service) hashLoginCode(loginID, code string) []byte {
	hash := hmac.New(sha256.New, s.cfg.LoginHashKey)
	_, _ = hash.Write([]byte(loginID))
	_, _ = hash.Write([]byte{'\n'})
	_, _ = hash.Write([]byte(code))
	return hash.Sum(nil)
}

func normalizeEmail(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	address, err := mail.ParseAddress(raw)
	if err != nil || address.Address != raw || len(raw) > 320 {
		return "", ErrInvalidEmail
	}
	return strings.ToLower(address.Address), nil
}

func loginCode() (string, error) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", fmt.Errorf("generate login code: %w", err)
	}
	return fmt.Sprintf("%06d", value.Int64()), nil
}
