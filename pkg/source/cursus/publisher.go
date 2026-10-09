package cursus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/tabellarius/pkg/model"
)

type Publisher struct {
	pub          publisherClient
	retryInitial time.Duration
	retryMax     time.Duration
	wait         func(context.Context, time.Duration) error
}

type PublisherOptions struct {
	AllowSingleReplica bool
}

type publisherClient interface {
	Send(message string) (uint64, error)
	Flush() error
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
	return p.PublishContext(context.Background(), evt)
}

func (p *Publisher) PublishContext(ctx context.Context, evt model.Event) error {
	if p.pub == nil {
		return fmt.Errorf("broker publisher not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	eventJSON, err := marshalEvent(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = p.pub.Send(string(eventJSON))
	if err != nil {
		return fmt.Errorf("failed to publish message to cursus: %w", err)
	}

	backoff := p.initialRetryBackoff()
	for attempt := 1; ; attempt++ {
		if err := p.pub.Flush(); err != nil {
			if !isRetryableDeliveryWait(err) {
				return fmt.Errorf("failed to publish message to cursus: %w", err)
			}
			delay := jitteredBackoff(backoff)
			log.Printf("component=cursus_publisher event=delivery_retry attempt=%d delay=%s error=%q", attempt, delay, err)
			if err := p.waitForRetry(ctx, delay); err != nil {
				return fmt.Errorf("wait for cursus acknowledgement: %w", err)
			}
			backoff = min(backoff*2, p.maximumRetryBackoff())
			continue
		}
		break
	}

	p.logEvent(evt)

	return nil
}

func (p *Publisher) initialRetryBackoff() time.Duration {
	if p.retryInitial > 0 {
		return p.retryInitial
	}
	return 250 * time.Millisecond
}

func (p *Publisher) maximumRetryBackoff() time.Duration {
	if p.retryMax > 0 {
		return p.retryMax
	}
	return 30 * time.Second
}

func (p *Publisher) waitForRetry(ctx context.Context, delay time.Duration) error {
	if p.wait != nil {
		return p.wait(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func jitteredBackoff(backoff time.Duration) time.Duration {
	if backoff <= 1 {
		return backoff
	}
	half := backoff / 2
	return half + time.Duration(rand.Int64N(int64(backoff-half)+1))
}

func isRetryableDeliveryWait(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sdk.ErrProducerOutcomeUnknown) {
		return false
	}
	var brokerErr *sdk.BrokerError
	if errors.As(err, &brokerErr) {
		return brokerErr.Retryable
	}
	return strings.Contains(strings.ToLower(err.Error()), "flush timeout")
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
