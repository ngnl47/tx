package metrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHandlerExposesMessageMetricsAndMailboxSnapshot(t *testing.T) {
	clear := RegisterMailboxSnapshot(func() (int, int, int) { return 3, 7, 1024 })
	defer clear()
	RecordAgentUplink(true, time.Millisecond)
	RecordAgentDownlink(2, time.Millisecond, nil)
	RecordMsgQPublish(PublishSession, 1, time.Millisecond, nil)
	RecordRPCSubmit(nil, time.Millisecond)
	RecordRPCStreamSend(time.Millisecond, 2*time.Millisecond)

	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics status = %d", response.Code)
	}
	body := response.Body.String()
	for _, want := range []string{
		`tx_agent_uplink_total{outcome="accepted"} `,
		`tx_agent_downlink_sent_total `,
		`tx_msgq_downlink_published_total{kind="session"} `,
		`tx_rpc_stream_submit_total{outcome="accepted"} `,
		`tx_rpc_stream_send_lock_seconds_count 1`,
		`tx_rpc_stream_send_seconds_count 1`,
		`tx_msgq_sessions 3`,
		`tx_msgq_backlog_messages 7`,
		`tx_msgq_backlog_bytes 1024`,
		`go_goroutines `,
		`process_cpu_seconds_total `,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestRecordersKeepPartialPublishAndFailedWriteDistinct(t *testing.T) {
	before := metricValues(t)
	RecordAgentDownlink(2, time.Millisecond, nil)
	RecordAgentDownlink(3, time.Millisecond, errors.New("socket closed"))
	RecordMsgQPublish(PublishRole, 2, time.Millisecond, errors.New("one mailbox overflowed"))
	after := metricValues(t)
	for metric, delta := range map[string]float64{
		"tx_agent_downlink_sent_total":                       2,
		"tx_agent_downlink_write_errors_total":               1,
		`tx_msgq_downlink_published_total{kind="role"}`:      2,
		`tx_msgq_downlink_publish_errors_total{kind="role"}`: 1,
	} {
		if got := after[metric] - before[metric]; got != delta {
			t.Errorf("%s delta = %v, want %v", metric, got, delta)
		}
	}
}

func metricValues(t *testing.T) map[string]float64 {
	t.Helper()
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	values := make(map[string]float64)
	for line := range strings.SplitSeq(response.Body.String(), "\n") {
		if !strings.HasPrefix(line, "tx_") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		values[parts[0]] = value
	}
	return values
}

func TestListenExposesMetricsOnExplicitAddress(t *testing.T) {
	listener, err := Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	response, err := http.Get("http://" + listener.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(content), "go_goroutines ") {
		t.Fatalf("metrics response = %d, %q", response.StatusCode, content)
	}
}

func TestMailboxSnapshotStopsAfterUnregister(t *testing.T) {
	clear := RegisterMailboxSnapshot(func() (int, int, int) { return 5, 8, 4096 })
	clear()
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), `tx_msgq_backlog_messages 0`) {
		t.Fatal("unregistered mailbox remains visible")
	}
}
