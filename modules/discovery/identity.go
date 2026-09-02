package discovery

import (
	"errors"
	"fmt"
	"strings"
)

const WorkSlotCount = 1<<12 - 1

var (
	ErrInvalidRuntimeIdentity = errors.New("discovery: invalid runtime identity")
	ErrWorkSlotExhausted      = errors.New("discovery: workSlot exhausted")
	ErrRuntimeClosed          = errors.New("discovery: runtime closed")
	ErrRuntimeIdentityLost    = errors.New("discovery: runtime identity lost")
)

// RuntimeIdentity 是进程本次运行期间持有的不可变身份。
type RuntimeIdentity struct {
	InstanceID  string
	WorkSlot    uint16
	ModRevision int64
}

func newRuntimeIdentity(appID string, workSlot uint16, modRevision int64) (RuntimeIdentity, error) {
	if appID == "" || appID != strings.TrimSpace(appID) || workSlot == 0 || workSlot > WorkSlotCount || modRevision <= 0 {
		return RuntimeIdentity{}, ErrInvalidRuntimeIdentity
	}
	return RuntimeIdentity{
		InstanceID:  fmt.Sprintf("%s-%d-%d", appID, workSlot, modRevision),
		WorkSlot:    workSlot,
		ModRevision: modRevision,
	}, nil
}

func workSlotAttempts(start uint16) []uint16 {
	attempts := make([]uint16, WorkSlotCount)
	for offset := range WorkSlotCount {
		attempts[offset] = uint16((int(start)-1+offset)%WorkSlotCount + 1)
	}
	return attempts
}
