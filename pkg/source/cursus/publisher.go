package cursus

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/tabellarius/pkg/model"
	"gopkg.in/yaml.v3"
)

type Publisher struct {
	pub publisherClient
}

type publisherClient interface {
	PublishMessage(message string) (uint64, error)
	Close() error
}

type eventPayload struct {
	Type      string            `json:"type"`
	Source    model.SourceType  `json:"source"`
	Offset    string            `json:"offset"`
	Timestamp string            `json:"timestamp"`
	TxID      string            `json:"tx_id,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Query     string            `json:"query,omitempty"`
	Changes   []model.RowChange `json:"changes,omitempty"`
}

func NewCursusPublisher(configPath string) (*Publisher, error) {
	if configPath == "" {
		configPath = "/config.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read publisher config %s: %w", configPath, err)
	}

	cfg := sdk.NewDefaultPublisherConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}

	pub, err := sdk.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cursus publisher: %w", err)
	}

	return &Publisher{
		pub: pub,
	}, nil
}

func (p *Publisher) Close() error {
	if p.pub != nil {
		return p.pub.Close()
	}
	return nil
}

func (p *Publisher) Publish(evt model.Event) error {
	if p.pub == nil {
		return fmt.Errorf("broker publisher not initialized")
	}

	p.logEvent(evt)

	eventJSON, err := marshalEvent(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = p.pub.PublishMessage(string(eventJSON))
	if err != nil {
		return fmt.Errorf("failed to publish message to cursus: %w", err)
	}

	return nil
}

func (p *Publisher) logEvent(evt model.Event) {
	prefix := fmt.Sprintf("[publish] source=%s offset=%s type=%T",
		evt.Source(), evt.Offset().String(), evt)

	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		log.Printf("%s [tx] kind=%s txID=%s", prefix, e.Kind(), e.TxID())

	case *model.BinlogDDLEvent:
		log.Printf("%s [ddl] txID=%s query=%s", prefix, e.TxID(), e.Query())

	case model.RowChangeEvent:
		for ci, change := range e.Changes() {
			for ri, row := range change.Rows {
				if change.Op == model.OpUpdate && row.Before != nil && row.After != nil {
					beforeJSON, _ := json.Marshal(row.Before)
					afterJSON, _ := json.Marshal(row.After)
					log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=UPDATE before=%s after=%s",
						prefix, ci, ri, change.Schema, change.Table, e.TxID(),
						string(beforeJSON), string(afterJSON))
				} else {
					log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=%s",
						prefix, ci, ri, change.Schema, change.Table, e.TxID(), change.Op)
				}
			}
		}

	default:
		log.Printf("%s [unknown event type]", prefix)
	}
}
func marshalEvent(evt model.Event) ([]byte, error) {
	payload := eventPayload{
		Source:    evt.Source(),
		Offset:    evt.Offset().String(),
		Timestamp: evt.Timestamp().UTC().Format(time.RFC3339Nano),
	}

	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		payload.Type = "transaction_boundary"
		payload.TxID = e.TxID()
		payload.Kind = string(e.Kind())
	case *model.BinlogDDLEvent:
		payload.Type = "ddl"
		payload.TxID = e.TxID()
		payload.Query = e.Query()
	case model.RowChangeEvent:
		payload.Type = "transaction"
		payload.TxID = e.TxID()
		payload.Changes = e.Changes()
	default:
		payload.Type = "unknown"
	}

	return json.Marshal(payload)
}
