package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cursus-io/tabellarius/pkg/bootstrap"
	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/health"
	"github.com/cursus-io/tabellarius/pkg/source"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	confPath := flag.String("config", "cdc-config.yaml", "config file path")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *confPath); err != nil {
		log.Printf("[FATAL] %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, confPath string) error {
	cfg, err := config.Load(confPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	status := health.NewStatus()
	healthAddress := cfg.CDCServer.HealthAddress
	if healthAddress == "" {
		healthAddress = ":8080"
	}
	healthServer := &http.Server{
		Addr:              healthAddress,
		Handler:           status.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	healthErr := make(chan error, 1)
	go func() {
		err := healthServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		healthErr <- err
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("[WARN] health server shutdown: %v", err)
		}
	}()

	connection, err := cfg.DatabaseConnection()
	if err != nil {
		return fmt.Errorf("prepare database connection: %w", err)
	}
	db, err := connectWithRetry(cfg.Database.Type.DriverName(), connection.DSN, 3)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("[WARN] database close: %v", err)
		}
	}()

	ok, err := bootstrap.Inspect(db, cfg)
	if err != nil {
		return fmt.Errorf("inspect CDC metadata: %w", err)
	}
	if !ok {
		return errors.New("cdc_log table not found; bootstrap required")
	}

	src, err := source.NewFromConfigWithStatus(db, cfg, status)
	if err != nil {
		return fmt.Errorf("initialize source: %w", err)
	}
	defer func() {
		if err := src.Close(); err != nil {
			log.Printf("[WARN] source close: %v", err)
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sourceErr := make(chan error, 1)
	go func() { sourceErr <- src.Run(runCtx) }()

	var runErr error
	sourceFinished := false
	select {
	case <-ctx.Done():
		log.Println("[INFO] shutdown requested")
	case err := <-sourceErr:
		sourceFinished = true
		if err != nil {
			runErr = err
		} else if ctx.Err() == nil {
			runErr = errors.New("CDC source stopped unexpectedly")
		}
	case err := <-healthErr:
		if err != nil {
			runErr = fmt.Errorf("health server: %w", err)
		} else if ctx.Err() == nil {
			runErr = errors.New("health server stopped unexpectedly")
		}
	}

	cancel()
	if !sourceFinished {
		if err := <-sourceErr; err != nil && runErr == nil && ctx.Err() == nil {
			runErr = err
		}
	}
	if runErr != nil {
		return runErr
	}
	log.Println("[OK] tabellarius stopped safely")
	return nil
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
				if closeErr := db.Close(); closeErr != nil {
					log.Printf("[WARN] failed database handle close: %v", closeErr)
				}
			}
		} else {
			lastErr = err
		}

		log.Printf("[WARN] db connection failed (attempt=%d/%d): %v", i, maxRetry, lastErr)
		time.Sleep(time.Duration(i) * time.Second)
	}

	return nil, fmt.Errorf("db connection failed after %d attempts: %w", maxRetry, lastErr)
}
