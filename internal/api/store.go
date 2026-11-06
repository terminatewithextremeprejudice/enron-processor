package api

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver on the database/sql API
)

// Sender is one row of the /top-senders response.
type Sender struct {
	Email        string `json:"email"`
	MessageCount int64  `json:"message_count"`
}

// Store is the read-only data surface the api needs. The api never writes
// to Postgres — writes belong to the parser (the sole writer); this is
// strictly a metrics projection over the schema it shares.
type Store interface {
	// TotalMessages is the count of durably parsed messages
	// (rows in the message table — the pipeline's effective-once sink).
	TotalMessages(ctx context.Context) (int64, error)

	// TopSenders returns the n most prolific senders by parsed-message
	// count, ties broken by email for a deterministic order.
	TopSenders(ctx context.Context, n int) ([]Sender, error)

	Ping(ctx context.Context) error
	Close() error
}

type pgStore struct {
	db *sql.DB
}

var _ Store = (*pgStore)(nil)

// NewStore opens the Postgres pool against dsn and verifies connectivity
// with a bounded ping. sql.Open is lazy, so without the Ping the first
// request would pay for a bad DSN.
func NewStore(ctx context.Context, dsn string) (Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &pgStore{db: db}, nil
}

// totalMessagesQuery is deliberately simple: count(*) over a table whose
// unique idempotency key makes redelivery a no-op downstream.
const totalMessagesQuery = `SELECT count(*) FROM message`

// topSendersQuery counts messages per sender person. The identity of a
// person is the email address (unique), so grouping by p.id is grouping by
// sender. Ties on count are broken by email ascending → the response is
// stable across calls, which matters for scripts diffing /top-senders.
const topSendersQuery = `
SELECT p.email, count(m.id) AS messages
  FROM message AS m
  JOIN person  AS p ON p.id = m.sender_id
 GROUP BY p.id, p.email
 ORDER BY messages DESC, p.email ASC
 LIMIT $1`

func (s *pgStore) TotalMessages(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, totalMessagesQuery).Scan(&n); err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return n, nil
}

func (s *pgStore) TopSenders(ctx context.Context, n int) ([]Sender, error) {
	if n <= 0 {
		n = defaultTopN
	}
	rows, err := s.db.QueryContext(ctx, topSendersQuery, n)
	if err != nil {
		return nil, fmt.Errorf("query top senders: %w", err)
	}
	defer rows.Close()

	senders := make([]Sender, 0, n)
	for rows.Next() {
		var s Sender
		if err := rows.Scan(&s.Email, &s.MessageCount); err != nil {
			return nil, fmt.Errorf("scan top senders: %w", err)
		}
		senders = append(senders, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate top senders: %w", err)
	}
	return senders, nil
}

func (s *pgStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *pgStore) Close() error { return s.db.Close() }
