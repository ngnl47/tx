//go:build !txbench

package agent

import "time"

func traceAgentDownlink(int64, []Frame, time.Time, time.Time) {}
