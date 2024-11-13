package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

const defaultMaxConns = int32(20)
const defaultMinConns = int32(0)
const defaultMaxConnLifetime = time.Hour
const defaultMaxConnIdleTime = time.Minute * 30
const defaultHealthCheckPeriod = time.Minute
const defaultConnectTimeout = time.Second * 5

// const numLoops = 1000

type Config struct {
	Query    string `yaml:"query"`
	DSN      string `yaml:"dsn"`
	Duration int    `yaml:"duration"`
}

func main() {
	var cfg *Config

	cfg, err := newConfig("config.yaml")
	if err != nil {
		fmt.Fprintln(os.Stderr, "ParseConfig failed:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := newPostgreSQL(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "PostgreSQL init error:", err)
		os.Exit(1)
	}

	defer func() {
		fmt.Println("Closing connections")

		// Postgres will deal with opend idle connections automatically
		// pool.Close()
	}()

	conns := make([]*pgxpool.Conn, defaultMaxConns)
	fmt.Printf("number of idle conns: %d\n", len(conns))
	for i := 0; i < int(defaultMaxConns); i++ {
		conns[i], err = establishConnection(ctx, pool)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error establishing connections: %v", err)
			os.Exit(1)
		}
	}

	var wg sync.WaitGroup
	var numErrors int32
	var counter int32
	var totalTime time.Duration
	// var elapsedTime time.Duration

	fmt.Println("Making requests")

	start := time.Now()
	// for loop := 0; loop < numLoops; loop++ {
	for totalTime < time.Millisecond*time.Duration(cfg.Duration) {
		wg.Add(int(defaultMaxConns))
		for i := 0; i < int(defaultMaxConns); i++ {
			conn := conns[i]
			go func() {
				defer wg.Done()

				_, err := makeRequest(ctx, conn, cfg)
				if err != nil {
					atomic.AddInt32(&numErrors, 1)
					// fmt.Fprintf(os.Stderr, "Error during request: %v\n", err)
				} else {
					atomic.AddInt32(&counter, 1)
					// totalTime += elapsed
				}
			}()
		}
		wg.Wait()
		totalTime = time.Since(start)
	}

	// fmt.Println("time elapsed:", elapsedTime)
	fmt.Println("total time:", totalTime)
	fmt.Println("requests made:", counter)
	fmt.Println("errors:", numErrors)
	fmt.Println("RPS:", float64(counter)/float64(totalTime.Seconds()))

	fmt.Println("Exiting ...")
}

func makeRequest(ctx context.Context, conn *pgxpool.Conn, cfg *Config) (time.Duration, error) {
	start := time.Now()

	_, err := conn.Exec(ctx, cfg.Query)
	if err != nil {
		return time.Duration(0), fmt.Errorf("error making request: %v", err)
	}

	elapsed := time.Since(start)
	return elapsed, nil
}

func establishConnection(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func newPostgreSQL(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	dbConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	return pool, nil
}

func newConfig(configPath string) (*Config, error) {
	var cfg Config

	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return &Config{}, err
	}

	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		return &Config{}, err
	}
	return &cfg, nil
}
