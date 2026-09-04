package source

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cursus-io/tabellarius/pkg/health"
	"github.com/cursus-io/tabellarius/pkg/inspector"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/util"
)

type eventPublisher interface {
	Publish(model.Event) error
	Close() error
}

type TabellariusSource struct {
	ins            inspector.Inspector[model.Event]
	pub            eventPublisher
	checkpointPath string
	status         *health.Status
}

func (s *TabellariusSource) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan model.Event, 128)
	inspectorErr := make(chan error, 1)
	go func() {
		err := s.ins.Start(runCtx, events)
		close(events)
		inspectorErr <- err
	}()

	processErr := s.run(runCtx, events)
	cancel()
	streamErr := <-inspectorErr

	if processErr != nil {
		s.status.StreamFailed()
		return processErr
	}
	if streamErr != nil && ctx.Err() == nil {
		s.status.StreamFailed()
		return fmt.Errorf("binlog stream failed: %w", streamErr)
	}
	return nil
}

func (s *TabellariusSource) Close() error {
	if s.pub != nil {
		return s.pub.Close()
	}
	return nil
}

func (s *TabellariusSource) run(ctx context.Context, in <-chan model.Event) error {
	txBuffer := map[string][]model.RowChange{}
	var lastOffset model.Offset
	var lastSource model.SourceType
	var eventCount uint64

	defer func() {
		log.Printf("Shutting down. Remaining transactions in buffer: %d", len(txBuffer))
	}()

	for {
		select {
		case <-ctx.Done():
			return nil

		case evt, ok := <-in:
			if !ok {
				return nil
			}

			lag := time.Since(evt.Timestamp())
			eventCount++
			s.status.EventProcessed(evt.Timestamp())
			if eventCount%1000 == 0 {
				log.Printf("[metrics] processed=%d current_lag=%v", eventCount, lag)
			}

			lastOffset = evt.Offset()
			lastSource = evt.Source()

			switch e := evt.(type) {
			case model.RowChangeEvent:
				txBuffer[e.TxID()] = append(txBuffer[e.TxID()], e.Changes()...)

			case *model.BinlogDDLEvent:
				if err := s.pub.Publish(e); err != nil {
					s.status.PublishFailed()
					return fmt.Errorf("publish DDL event: %w", err)
				}
				if err := s.saveCheckpoint(lastOffset); err != nil {
					return err
				}

			case *model.TransactionBoundaryEvent:
				switch e.Kind() {
				case model.TxCommit:
					changes := txBuffer[e.TxID()]
					if len(changes) > 0 {
						txEvt := model.NewTransactionEvent(lastSource, lastOffset, e.Timestamp(), e.TxID(), changes)
						if err := s.pub.Publish(txEvt); err != nil {
							s.status.PublishFailed()
							return fmt.Errorf("publish transaction %s: %w", e.TxID(), err)
						}
					}
					delete(txBuffer, e.TxID())
					if err := s.saveCheckpoint(lastOffset); err != nil {
						return err
					}

				case model.TxRollback:
					delete(txBuffer, e.TxID())
					if err := s.saveCheckpoint(lastOffset); err != nil {
						return err
					}
				}
			}
		}
	}
}

func (s *TabellariusSource) saveCheckpoint(offset model.Offset) error {
	mysqlOffset, ok := offset.(model.MySQLOffset)
	if !ok {
		return fmt.Errorf("unsupported checkpoint type %T", offset)
	}
	if err := util.SaveJSON(s.checkpointPath, mysqlOffset); err != nil {
		s.status.CheckpointFailed()
		return fmt.Errorf("save checkpoint: %w", err)
	}
	s.status.CheckpointSaved()
	return nil
}
