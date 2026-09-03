package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/cursus-io/tabellarius/pkg/model"
	"gopkg.in/yaml.v3"
)

type Database struct {
	Type     model.DatabaseType `yaml:"type"`
	Schema   string             `yaml:"schema"`
	User     string             `yaml:"user"`
	Password string             `yaml:"password"`
	Host     string             `yaml:"host"`
	Port     int                `yaml:"port"`
}

type Table struct {
	Name           string   `yaml:"name"`
	PK             string   `yaml:"pk"`
	IncludeColumns []string `yaml:"include_columns,omitempty"`
	ExcludeColumns []string `yaml:"exclude_columns,omitempty"`
}

type CDCServer struct {
	ServerID        uint32 `yaml:"server_id"`
	OffsetFile      string `yaml:"offset_file"`
	PublisherConfig string `yaml:"publisher_config"`
}

type Config struct {
	Database Database `yaml:"database"`
	CdcLog   struct {
		Table string `yaml:"table"`
	} `yaml:"cdc_log"`
	Tables    []Table   `yaml:"tables"`
	CDCServer CDCServer `yaml:"cdc_server"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	password, err := resolveEnvironmentReference(c.Database.Password)
	if err != nil {
		return nil, err
	}
	c.Database.Password = password
	return &c, nil
}

func resolveEnvironmentReference(value string) (string, error) {
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return value, nil
	}
	name := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
	if name == "" || strings.ContainsAny(name, "${}") {
		return value, nil
	}
	expanded, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return expanded, nil
}

func (c *Config) DSN() string {
	db := c.Database

	switch db.Type {
	case model.MySQL, model.MariaDB:
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=skip-verify", db.User, db.Password, db.Host, db.Port, db.Schema)

	case model.Postgres:
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.User, db.Password, db.Host, db.Port, db.Schema)

	default:
		panic("unsupported database type: " + string(db.Type))
	}
}
