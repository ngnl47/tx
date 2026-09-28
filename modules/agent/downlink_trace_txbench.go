//go:build txbench

package agent

import (
	"io"
	"os"
	"strconv"
	"time"
)

var agentDownlinkTrace = openAgentDownlinkTrace()

func openAgentDownlinkTrace() io.Writer {
	path := os.Getenv("TX_AGENT_DOWNLINK_TRACE_FILE")
	if path == "" {
		return io.Discard
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	return file
}

func traceAgentDownlink(roleID int64, frames []Frame, started, ended time.Time) {
	if agentDownlinkTrace == io.Discard || roleID%32 != 0 {
		return
	}
	writeAgentDownlinkTrace(agentDownlinkTrace, roleID, frames, started, ended)
}

func writeAgentDownlinkTrace(writer io.Writer, roleID int64, frames []Frame, started, ended time.Time) {
	if roleID%32 != 0 {
		return
	}
	for _, frame := range frames {
		if frame.Seq%8 != 0 {
			continue
		}
		var line [96]byte
		out := strconv.AppendInt(line[:0], roleID, 10)
		out = append(out, ',')
		out = strconv.AppendInt(out, frame.Seq, 10)
		out = append(out, ',')
		out = strconv.AppendInt(out, started.UnixMicro(), 10)
		out = append(out, ',')
		out = strconv.AppendInt(out, ended.UnixMicro(), 10)
		out = append(out, '\n')
		_, _ = writer.Write(out)
	}
}
