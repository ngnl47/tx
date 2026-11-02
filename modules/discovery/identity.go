package discovery

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

var (
	ErrInvalidRuntimeIdentity = errors.New("discovery: invalid runtime identity")
	ErrRuntimeClosed          = errors.New("discovery: runtime closed")
	ErrRuntimeIdentityLost    = errors.New("discovery: runtime identity lost")
)

// RuntimeIdentity 是进程本次运行期间持有的不可变身份，InstanceID 不用于解析业务信息。
type RuntimeIdentity struct {
	InstanceID string
}

func newRuntimeIdentity(appID string) (RuntimeIdentity, error) {
	if appID == "" || appID != strings.TrimSpace(appID) {
		return RuntimeIdentity{}, ErrInvalidRuntimeIdentity
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return RuntimeIdentity{}, err
	}
	return RuntimeIdentity{InstanceID: appID + "-" + id.String()}, nil
}
