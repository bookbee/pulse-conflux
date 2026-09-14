// Package redis implements the ingestion boundary against the gateway's queue.
//
// This is the ONLY package in the module permitted to import a Redis client;
// test/integration/boundary_test.go fails the build otherwise. Everything
// Redis-shaped — consumer groups, pending entries, claims, the list's
// destructive read — stops here (Constitution Principle II).
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/config"
)

// Client wraps the go-redis client with this service's connection policy.
type Client struct {
	rdb *goredis.Client
	log *slog.Logger
}

// NewClient dials Redis and verifies the connection.
//
// Connection failures back off rather than spinning: an unreachable or
// auth-refusing Redis must not turn into a hot loop against it.
func NewClient(ctx context.Context, cfg config.Redis, log *slog.Logger) (*Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		MaxRetries:      3,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 2 * time.Second,
		DialTimeout:     5 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: connect %s db %d: %w", cfg.Addr, cfg.DB, err)
	}

	log.Info("redis connected",
		slog.String("component", "redis"),
		slog.String("addr", cfg.Addr),
		slog.Int("db", cfg.DB))
	return &Client{rdb: rdb, log: log}, nil
}

// Ping reports whether Redis is reachable. Used by readiness, never liveness.
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// Close releases the connection pool.
func (c *Client) Close() error { return c.rdb.Close() }

// Raw exposes the underlying client to this package's siblings only.
func (c *Client) Raw() *goredis.Client { return c.rdb }
