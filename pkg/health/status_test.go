package health

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadyTracksStreamLifecycle(t *testing.T) {
	status := NewStatus()
	handler := status.Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial readiness = %d", recorder.Code)
	}

	status.StreamStarted()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("started readiness = %d", recorder.Code)
	}

	status.StreamFailed()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("terminal liveness = %d", recorder.Code)
	}
}

func TestMetricsContainNoEventData(t *testing.T) {
	status := NewStatus()
	status.StreamStarted()
	status.BinlogEventReceived(512)
	status.RowImagesReceived(5)
	status.RowImagesCaptured(2)
	status.RowImagesFiltered(3)
	status.EventProcessed(time.Now().Add(-2 * time.Second))
	status.CheckpointSaved()
	status.PublishFailed()

	recorder := httptest.NewRecorder()
	status.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, metric := range []string{
		"tabellarius_stream_ready 1",
		"tabellarius_binlog_events_received_total 1",
		"tabellarius_binlog_bytes_received_total 512",
		"tabellarius_row_images_received_total 5",
		"tabellarius_row_images_captured_total 2",
		"tabellarius_row_images_filtered_total 3",
		"tabellarius_processed_events_total 1",
		"tabellarius_publish_failures_total 1",
		"tabellarius_last_checkpoint_timestamp_seconds",
		"tabellarius_source_event_lag_seconds",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics missing %q: %s", metric, body)
		}
	}
}
