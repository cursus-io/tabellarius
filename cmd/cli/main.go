package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/cursus-io/tabellarius/pkg/bootstrap"
	"github.com/cursus-io/tabellarius/pkg/config"
	_ "github.com/go-sql-driver/mysql"
)

const (
	ModeInspect = "inspect"
	ModeInit    = "init"
)

type RunOptions struct {
	Mode  string
	Apply bool
}

func Run(cfg *config.Config, db *sql.DB, opt RunOptions) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	switch opt.Mode {
	case ModeInspect:
		_, err := bootstrap.Inspect(db, cfg)
		return err

	case ModeInit:
		if !opt.Apply {
			return fmt.Errorf("--apply flag required for init mode")
		}
		return bootstrap.Init(db, cfg)

	default:
		return fmt.Errorf("unknown mode: %s", opt.Mode)
	}
}

func main() {
	var (
		mode  = flag.String("mode", ModeInspect, ModeInspect+"|"+ModeInit)
		conf  = flag.String("config", "cdc-config.yaml", "config path")
		apply = flag.Bool("apply", false, "apply changes")
	)
	flag.Parse()

	cfg, err := config.Load(*conf)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	driver := cfg.Database.Type.DriverName()
	connection, err := cfg.DatabaseConnection()
	if err != nil {
		log.Fatalf("failed to prepare database connection: %v", err)
	}
	db, err := sql.Open(driver, connection.DSN)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	log.Printf("mode=%s apply=%v", *mode, *apply)

	if err := Run(cfg, db, RunOptions{Mode: *mode, Apply: *apply}); err != nil {
		log.Fatal(err)
	}
}
