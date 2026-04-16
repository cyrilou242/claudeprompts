package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	host := flag.String("host", envOr("CH_HOST", "localhost"), "ClickHouse host")
	port := flag.Int("port", envOrInt("CH_PORT", 9000), "ClickHouse native port")
	user := flag.String("user", envOr("CH_USER", "default"), "ClickHouse user")
	password := flag.String("password", envOr("CH_PASSWORD", ""), "ClickHouse password")
	database := flag.String("database", envOr("CH_DATABASE", "claude_code"), "ClickHouse database")
	table := flag.String("table", envOr("CH_TABLE", "raw"), "ClickHouse table")
	secure := flag.Bool("secure", envOr("CH_SECURE", "") != "", "Use TLS for ClickHouse connection")
	claudeDir := flag.String("claude-dir", envOr("CLAUDE_DIR", filepath.Join(homeDir(), ".claude")), "Claude data directory")
	interval := flag.Duration("interval", envOrDuration("SCAN_INTERVAL", 10*time.Minute), "Scan interval")
	metadata := flag.String("metadata", envOr("CH_METADATA", "{}"), `JSON metadata attached to every row (e.g. '{"user":"cyril","team":"eng"}')`)
	once := flag.Bool("once", false, "Run once and exit")
	flag.Parse()

	// Validate metadata is valid JSON
	if !json.Valid([]byte(*metadata)) {
		log.Fatalf("invalid JSON in -metadata: %s", *metadata)
	}

	// Prevent duplicate instances
	if err := acquireLock(); err != nil {
		log.Fatalf("another instance is already running: %v", err)
	}
	defer releaseLock()

	// Verify clickhouse binary exists
	chBin := findClickHouseBinary()
	if chBin == "" {
		log.Fatal("clickhouse binary not found in PATH (need clickhouse-local and clickhouse-client, or the unified 'clickhouse' binary)")
	}
	log.Printf("using clickhouse binary: %s", chBin)

	clientArgs := []string{
		"--host", *host,
		"--port", fmt.Sprintf("%d", *port),
		"--user", *user,
		"--password", *password,
	}
	if *secure {
		clientArgs = append(clientArgs, "--secure")
	}

	// Ensure schema exists
	log.Println("ensuring schema...")
	if err := ensureSchema(chBin, clientArgs, *database, *table); err != nil {
		log.Fatalf("schema setup failed: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("starting ingestion loop (interval=%s, once=%v)", *interval, *once)
	log.Printf("scanning: %s", *claudeDir)
	log.Printf("target: %s:%d/%s.%s", *host, *port, *database, *table)

	for {
		t0 := time.Now()
		n, err := ingest(chBin, clientArgs, *claudeDir, *database, *table, *metadata)
		elapsed := time.Since(t0)

		if err != nil {
			log.Printf("scan failed (%s): %v", elapsed.Round(time.Millisecond), err)
		} else if n > 0 {
			log.Printf("scan complete: %d globs processed in %s", n, elapsed.Round(time.Millisecond))
		}

		if *once {
			return
		}

		select {
		case <-sig:
			log.Println("shutting down")
			return
		case <-time.After(*interval):
		}
	}
}

func ingest(chBin string, clientArgs []string, claudeDir, database, table, metadata string) (int, error) {
	globs := []string{
		filepath.Join(claudeDir, "projects", "*", "*.jsonl"),
		filepath.Join(claudeDir, "projects", "*", "*", "subagents", "*.jsonl"),
		filepath.Join(claudeDir, "history.jsonl"),
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s.%s FORMAT Native", database, table)
	count := 0

	for _, pattern := range globs {
		// Check if any files match before running clickhouse-local
		matches, _ := filepath.Glob(pattern)
		if len(matches) == 0 {
			continue
		}

		localQuery := fmt.Sprintf(`SELECT _path AS path, json AS data, '%s' AS metadata FROM file('%s', 'JSONAsString') WHERE isValidJSON(json) SETTINGS input_format_allow_errors_ratio=0.1 FORMAT Native`, metadata, pattern)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		localCmd := exec.CommandContext(ctx, chBin, "local", "-q", localQuery)
		clientCmd := exec.CommandContext(ctx, chBin, append([]string{"client"}, append(clientArgs, "-q", insertQuery)...)...)

		pipe, err := localCmd.StdoutPipe()
		if err != nil {
			return count, fmt.Errorf("pipe setup: %w", err)
		}
		clientCmd.Stdin = pipe

		var localStderr, clientStderr strings.Builder
		localCmd.Stderr = &localStderr
		clientCmd.Stderr = &clientStderr

		if err := localCmd.Start(); err != nil {
			return count, fmt.Errorf("clickhouse local start: %w", err)
		}
		if err := clientCmd.Start(); err != nil {
			localCmd.Process.Kill()
			return count, fmt.Errorf("clickhouse client start: %w", err)
		}

		localErr := localCmd.Wait()
		clientErr := clientCmd.Wait()
		cancel()

		if localErr != nil {
			// input_format_allow_errors_ratio means some errors are expected
			if !strings.Contains(localStderr.String(), "Code: 27") {
				return count, fmt.Errorf("clickhouse local: %v: %s", localErr, localStderr.String())
			}
		}
		if clientErr != nil {
			return count, fmt.Errorf("clickhouse client: %v: %s", clientErr, clientStderr.String())
		}

		count++
	}

	return count, nil
}

func ensureSchema(chBin string, clientArgs []string, database, table string) error {
	ddl := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (path String, data JSON, metadata JSON) ENGINE = ReplacingMergeTree ORDER BY (data.sessionId::String, data.timestamp::String, data.uuid::String)`, database, table),
	}

	for _, stmt := range ddl {
		cmd := exec.Command(chBin, append([]string{"client"}, append(clientArgs, "-q", stmt)...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s: %v: %s", stmt[:40], err, string(out))
		}
	}
	return nil
}

func findClickHouseBinary() string {
	// Prefer unified 'clickhouse' binary
	if p, err := exec.LookPath("clickhouse"); err == nil {
		return p
	}
	// Fall back to separate binaries — check both exist
	local, errL := exec.LookPath("clickhouse-local")
	_, errC := exec.LookPath("clickhouse-client")
	if errL == nil && errC == nil {
		// Return the prefix; we'll need to handle this differently
		// Actually the unified binary uses "clickhouse local" / "clickhouse client"
		// while separate binaries are "clickhouse-local" / "clickhouse-client"
		_ = local
	}
	return ""
}

func homeDir() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return os.Getenv("HOME")
	}
	return h
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		// Try as plain seconds
		var s int
		if _, err := fmt.Sscanf(v, "%d", &s); err == nil {
			return time.Duration(s) * time.Second
		}
	}
	return fallback
}

var lockFile *os.File

func acquireLock() error {
	lockPath := filepath.Join(os.TempDir(), "claudeprompts-ingest.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return fmt.Errorf("lock held by another process")
	}
	f.Truncate(0)
	fmt.Fprintf(f, "%d", os.Getpid())
	lockFile = f
	return nil
}

func releaseLock() {
	if lockFile != nil {
		syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		lockFile.Close()
		os.Remove(lockFile.Name())
	}
}
