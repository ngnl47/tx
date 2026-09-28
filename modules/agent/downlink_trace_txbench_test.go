//go:build txbench

package agent

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWriteAgentDownlinkTraceSamplesMatchingFrame(t *testing.T) {
	var output bytes.Buffer
	started := time.UnixMicro(1_000_000)
	ended := started.Add(250 * time.Microsecond)
	writeAgentDownlinkTrace(&output, 480096, []Frame{{Seq: 7}, {Seq: 8}, {Seq: 9}}, started, ended)
	if got := strings.TrimSpace(output.String()); got != "480096,8,1000000,1000250" {
		t.Fatalf("trace = %q", got)
	}
	output.Reset()
	writeAgentDownlinkTrace(&output, 480097, []Frame{{Seq: 8}}, started, ended)
	if output.Len() != 0 {
		t.Fatalf("unexpected trace = %q", output.String())
	}
}
