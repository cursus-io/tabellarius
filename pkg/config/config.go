package config

import (
	"fmt"
	"os"

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
	Name string `yaml:"name"`
	PK   string `yaml:"pk"`
}

type CDCServer struct {
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
	return &c, yaml.Unmarshal(b, &c)
}

func (c *Config) DSN() string {
	db := c.Database

	switch db.Type {
	case model.MySQL, model.MariaDB:
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", db.User, db.Password, db.Host, db.Port, db.Schema)

	case model.Postgres:
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.User, db.Password, db.Host, db.Port, db.Schema)

	default:
		panic("unsupported database type: " + string(db.Type))
	}
}
