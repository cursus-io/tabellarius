package cursus

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/tabellarius/pkg/model"
)

type Publisher struct {
	pub publisherClient
}

type PublisherOptions struct {
	AllowSingleReplica bool
}

type publisherClient interface {
	Send(message string) (uint64, error)
	Flush()
	GetUniqueAckCount() uint64
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
	return NewCursusPublisherWithOptions(configPath, PublisherOptions{})
}

func NewCursusPublisherWithOptions(configPath string, options PublisherOptions) (*Publisher, error) {
	if configPath == "" {
		configPath = "/config.yaml"
	}

	cfg, err := loadPublisherConfig(configPath)
	if err != nil {
		return nil, err
	}
	if !options.AllowSingleReplica {
		if cfg.AutoCreateTopics {
			return nil, fmt.Errorf("auto_create_topics must be false for CDC publishing")
		}
		if cfg.Acks != "all" && cfg.Acks != "-1" {
			return nil, fmt.Errorf("CDC publishing requires acks=all, got %q", cfg.Acks)
		}
		if !cfg.EnableIdempotence {
			return nil, fmt.Errorf("CDC publishing requires enable_idempotence=true")
		}
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

	eventJSON, err := marshalEvent(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	ackCount := p.pub.GetUniqueAckCount()
	_, err = p.pub.Send(string(eventJSON))
	if err != nil {
		return fmt.Errorf("failed to publish message to cursus: %w", err)
	}
	p.pub.Flush()
	if p.pub.GetUniqueAckCount() <= ackCount {
		return fmt.Errorf("failed to publish message to cursus: broker acknowledgement was not received")
	}

	p.logEvent(evt)

	return nil
}

func (p *Publisher) logEvent(evt model.Event) {
	prefix := fmt.Sprintf("[publish] source=%s offset=%s type=%T",
		evt.Source(), evt.Offset().String(), evt)

	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		log.Printf("%s [tx] kind=%s txID=%s", prefix, e.Kind(), e.TxID())

	case *model.BinlogDDLEvent:
		log.Printf("%s [ddl] txID=%s", prefix, e.TxID())

	case model.RowChangeEvent:
		for _, change := range e.Changes() {
			log.Printf("%s [rows] table=%s.%s op=%s rows=%d txID=%s",
				prefix, change.Schema, change.Table, change.Op, len(change.Rows), e.TxID())
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
