package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cursus-io/tabellarius/pkg/bootstrap"
	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/source"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	confPath := flag.String("config", "cdc-config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*confPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := connectWithRetry(cfg.Database.Type.DriverName(), cfg.DSN(), 3)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}
	defer db.Close()

	log.Println("[OK] db connected")

	ok, err := bootstrap.Inspect(db, cfg)
	if err != nil {
		log.Fatalf("[FATAL] inspect failed: %v", err)
	}

	if !ok {
		log.Fatalf("[FATAL] cdc_log table not found. bootstrap required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	src, err := source.NewFromConfig(db, cfg)
	if err != nil {
		log.Fatalf("[FATAL] failed to initialize source: %v", err)
	}
	src.Start(ctx)

	sig := <-sigChan
	log.Printf("[INFO] received signal (%s). starting graceful shutdown...", sig)

	cancel()

	if err := src.Close(); err != nil {
		log.Printf("[WARN] error closing source: %v", err)
	}

	time.Sleep(2 * time.Second)
	log.Println("[OK] tabellarius stopped safely.")
}

func connectWithRetry(driver, dsn string, maxRetry int) (*sql.DB, error) {
	var lastErr error

	for i := 1; i <= maxRetry; i++ {
		db, err := sql.Open(driver, dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				log.Printf("[OK] db connected (attempt=%d)", i)
				return db, nil
			} else {
				lastErr = pingErr
				db.Close()
			}
		} else {
			lastErr = err
		}

		log.Printf("[WARN] db connection failed (attempt=%d/%d): %v", i, maxRetry, lastErr)
		time.Sleep(time.Duration(i) * time.Second)
	}

	return nil, fmt.Errorf("db connection failed after %d attempts: %w", maxRetry, lastErr)
}
