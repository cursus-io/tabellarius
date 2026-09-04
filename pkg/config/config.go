package config

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cursus-io/tabellarius/pkg/model"
	mysqldriver "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

var registeredTLSConfigs sync.Map

type Database struct {
	Type          model.DatabaseType `yaml:"type"`
	Schema        string             `yaml:"schema"`
	User          string             `yaml:"user"`
	Password      string             `yaml:"password"`
	Host          string             `yaml:"host"`
	Port          int                `yaml:"port"`
	TLSMode       string             `yaml:"tls_mode,omitempty"`
	TLSCAFile     string             `yaml:"tls_ca_file,omitempty"`
	TLSServerName string             `yaml:"tls_server_name,omitempty"`
}

type DatabaseConnection struct {
	DSN       string
	TLSConfig *tls.Config
}

type Table struct {
	Name           string   `yaml:"name"`
	PK             string   `yaml:"pk"`
	IncludeColumns []string `yaml:"include_columns,omitempty"`
	ExcludeColumns []string `yaml:"exclude_columns,omitempty"`
}

type CDCServer struct {
	ServerID                    uint32 `yaml:"server_id"`
	OffsetFile                  string `yaml:"offset_file"`
	PublisherConfig             string `yaml:"publisher_config"`
	RequireExistingCheckpoint   bool   `yaml:"require_existing_checkpoint,omitempty"`
	RequireGTID                 bool   `yaml:"require_gtid,omitempty"`
	AllowSingleReplicaPublisher bool   `yaml:"allow_single_replica_publisher,omitempty"`
	CaptureDDL                  bool   `yaml:"capture_ddl,omitempty"`
	HealthAddress               string `yaml:"health_address,omitempty"`
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

func (c *Config) DatabaseConnection() (*DatabaseConnection, error) {
	db := c.Database

	switch db.Type {
	case model.MySQL, model.MariaDB:
		tlsConfig, tlsName, err := db.mysqlTLSConfig()
		if err != nil {
			return nil, err
		}
		driverConfig := mysqldriver.NewConfig()
		driverConfig.User = db.User
		driverConfig.Passwd = db.Password
		driverConfig.Net = "tcp"
		driverConfig.Addr = fmt.Sprintf("%s:%d", db.Host, db.Port)
		driverConfig.DBName = db.Schema
		driverConfig.ParseTime = true
		driverConfig.TLSConfig = tlsName
		return &DatabaseConnection{DSN: driverConfig.FormatDSN(), TLSConfig: tlsConfig}, nil

	case model.Postgres:
		return &DatabaseConnection{DSN: fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.User, db.Password, db.Host, db.Port, db.Schema)}, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", db.Type)
	}
}

func (db Database) mysqlTLSConfig() (*tls.Config, string, error) {
	mode := strings.ToLower(strings.TrimSpace(db.TLSMode))
	if mode == "" {
		mode = "required"
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	var caPEM []byte
	switch mode {
	case "required":
		tlsConfig.InsecureSkipVerify = true //nolint:gosec // Explicit encryption-only compatibility mode.
	case "verify_ca", "verify_identity":
		if strings.TrimSpace(db.TLSCAFile) == "" {
			return nil, "", fmt.Errorf("tls_ca_file is required for tls_mode %s", mode)
		}
		var err error
		caPEM, err = os.ReadFile(db.TLSCAFile)
		if err != nil {
			return nil, "", fmt.Errorf("read database TLS CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, "", errors.New("database TLS CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
		if mode == "verify_identity" {
			tlsConfig.ServerName = strings.TrimSpace(db.TLSServerName)
			if tlsConfig.ServerName == "" {
				tlsConfig.ServerName = db.Host
			}
		} else {
			tlsConfig.InsecureSkipVerify = true //nolint:gosec // Chain verification is performed below without hostname matching.
			tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return errors.New("database TLS peer did not provide a certificate")
				}
				intermediates := x509.NewCertPool()
				for _, certificate := range state.PeerCertificates[1:] {
					intermediates.AddCert(certificate)
				}
				_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates})
				return err
			}
		}
	default:
		return nil, "", fmt.Errorf("unsupported database tls_mode %q", db.TLSMode)
	}

	identity := sha256.Sum256(append(append([]byte(mode+"\x00"+tlsConfig.ServerName+"\x00"), caPEM...), []byte(db.Host)...))
	name := fmt.Sprintf("tabellarius-%x", identity[:8])
	if _, loaded := registeredTLSConfigs.LoadOrStore(name, struct{}{}); !loaded {
		if err := mysqldriver.RegisterTLSConfig(name, tlsConfig.Clone()); err != nil {
			registeredTLSConfigs.Delete(name)
			return nil, "", fmt.Errorf("register database TLS config: %w", err)
		}
	}
	return tlsConfig, name, nil
}
