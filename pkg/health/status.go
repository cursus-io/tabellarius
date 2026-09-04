package health

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Status struct {
	ready                atomic.Bool
	terminal             atomic.Bool
	streamStarts         atomic.Uint64
	binlogEventsReceived atomic.Uint64
	binlogBytesReceived  atomic.Uint64
	rowImagesReceived    atomic.Uint64
	rowImagesCaptured    atomic.Uint64
	rowImagesFiltered    atomic.Uint64
	processedEvents      atomic.Uint64
	publishFailures      atomic.Uint64
	checkpointFailures   atomic.Uint64
	streamFailures       atomic.Uint64
	lastCheckpointUnix   atomic.Int64
	lastEventUnix        atomic.Int64
	lastEventLagNanosecs atomic.Int64
}

func (s *Status) BinlogEventReceived(bytes uint64) {
	if s == nil {
		return
	}
	s.binlogEventsReceived.Add(1)
	s.binlogBytesReceived.Add(bytes)
}

func (s *Status) RowImagesReceived(count uint64) {
	if s != nil {
		s.rowImagesReceived.Add(count)
	}
}

func (s *Status) RowImagesCaptured(count uint64) {
	if s != nil {
		s.rowImagesCaptured.Add(count)
	}
}

func (s *Status) RowImagesFiltered(count uint64) {
	if s != nil {
		s.rowImagesFiltered.Add(count)
	}
}

func NewStatus() *Status {
	return &Status{}
}

func (s *Status) StreamStarted() {
	if s == nil {
		return
	}
	s.streamStarts.Add(1)
	s.terminal.Store(false)
	s.ready.Store(true)
}

func (s *Status) StreamFailed() {
	if s == nil {
		return
	}
	s.streamFailures.Add(1)
	s.terminal.Store(true)
	s.ready.Store(false)
}

func (s *Status) EventProcessed(timestamp time.Time) {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.processedEvents.Add(1)
	s.lastEventUnix.Store(timestamp.UTC().Unix())
	lag := now.Sub(timestamp.UTC())
	if lag < 0 {
		lag = 0
	}
	s.lastEventLagNanosecs.Store(lag.Nanoseconds())
}

func (s *Status) PublishFailed() {
	if s != nil {
		s.publishFailures.Add(1)
	}
}

func (s *Status) CheckpointSaved() {
	if s != nil {
		s.lastCheckpointUnix.Store(time.Now().UTC().Unix())
	}
}

func (s *Status) CheckpointFailed() {
	if s != nil {
		s.checkpointFailures.Add(1)
	}
}

func (s *Status) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.handleLive)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func (s *Status) handleLive(w http.ResponseWriter, _ *http.Request) {
	if s.terminal.Load() {
		http.Error(w, "terminal failure", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Status) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() || s.terminal.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Status) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	ready := 0
	if s.ready.Load() && !s.terminal.Load() {
		ready = 1
	}
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_stream_ready gauge\ntabellarius_stream_ready %d\n", ready)
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_stream_starts_total counter\ntabellarius_stream_starts_total %d\n", s.streamStarts.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_binlog_events_received_total counter\ntabellarius_binlog_events_received_total %d\n", s.binlogEventsReceived.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_binlog_bytes_received_total counter\ntabellarius_binlog_bytes_received_total %d\n", s.binlogBytesReceived.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_row_images_received_total counter\ntabellarius_row_images_received_total %d\n", s.rowImagesReceived.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_row_images_captured_total counter\ntabellarius_row_images_captured_total %d\n", s.rowImagesCaptured.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_row_images_filtered_total counter\ntabellarius_row_images_filtered_total %d\n", s.rowImagesFiltered.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_processed_events_total counter\ntabellarius_processed_events_total %d\n", s.processedEvents.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_publish_failures_total counter\ntabellarius_publish_failures_total %d\n", s.publishFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_checkpoint_failures_total counter\ntabellarius_checkpoint_failures_total %d\n", s.checkpointFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_stream_failures_total counter\ntabellarius_stream_failures_total %d\n", s.streamFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_last_checkpoint_timestamp_seconds gauge\ntabellarius_last_checkpoint_timestamp_seconds %d\n", s.lastCheckpointUnix.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_last_event_timestamp_seconds gauge\ntabellarius_last_event_timestamp_seconds %d\n", s.lastEventUnix.Load())
	_, _ = fmt.Fprintf(w, "# TYPE tabellarius_source_event_lag_seconds gauge\ntabellarius_source_event_lag_seconds %.6f\n", float64(s.lastEventLagNanosecs.Load())/float64(time.Second))
}
