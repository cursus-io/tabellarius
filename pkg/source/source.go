package source

import (
	"context"
	"log"
	"time"

	"github.com/cursus-io/tabellarius/pkg/inspector"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/source/cursus"
)

type TabellariusSource struct {
	ins inspector.Inspector[model.Event]
	pub *cursus.Publisher
}

func (s *TabellariusSource) Start(ctx context.Context) {
	ch := make(chan model.Event, 128)

	go func() {
		defer close(ch)
		if err := s.ins.Start(ctx, ch); err != nil && ctx.Err() == nil {
			log.Printf("[binlog] stream failed: %v", err)
		}
	}()

	go s.run(ctx, ch)
}

func (s *TabellariusSource) Close() error {
	if s.pub != nil {
		return s.pub.Close()
	}
	return nil
}

func (s *TabellariusSource) run(ctx context.Context, in <-chan model.Event) {
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
			log.Println("Context cancelled, stopping...")
			return

		case evt, ok := <-in:
			if !ok {
				log.Println("Input channel closed, exiting loop")
				return
			}

			lag := time.Since(evt.Timestamp())
			eventCount++

			// Log metrics periodically (every 1000 events)
			if eventCount%1000 == 0 {
				log.Printf("[metrics] Processed: %d, Current Lag: %v", eventCount, lag)
			}

			lastOffset = evt.Offset()
			lastSource = evt.Source()

			switch e := evt.(type) {
			case model.RowChangeEvent:
				txBuffer[e.TxID()] = append(txBuffer[e.TxID()], e.Changes()...)
			case *model.BinlogDDLEvent:
				log.Printf("[schema] DDL Detected: %s (Offset: %v)", e.Query(), lastOffset)
				_ = s.pub.Publish(e)
			case *model.TransactionBoundaryEvent:
				switch e.Kind() {
				case model.TxCommit:
					// On Commit, bundle all buffered changes into a single transaction
					changes := txBuffer[e.TxID()]
					if len(changes) == 0 {
						delete(txBuffer, e.TxID())
						continue
					}

					txEvt := model.NewTransactionEvent(lastSource, lastOffset, e.Timestamp(), e.TxID(), changes)
					if err := s.pub.Publish(txEvt); err != nil {
						log.Printf("[run] Publish error for TxID %s: %v", e.TxID(), err)
					}
					delete(txBuffer, e.TxID())

				case model.TxRollback:
					delete(txBuffer, e.TxID())
				}
			}
		}
	}
}
