package actorresult

import (
	"errors"
	"testing"

	"github.com/ngnl47/tx/internal/actorhost"
)

func TestRemoteRejectionKeepsPublicErrorClassification(t *testing.T) {
	_, err := FromProto(nil, actorhost.RemoteErrorFrom(actorhost.ErrDraining))
	var call *CallError
	if !errors.Is(err, actorhost.ErrDraining) || !errors.As(err, &call) || call.Code != CallErrorCodeRejected {
		t.Fatalf("远端拒绝类别丢失：%v", err)
	}
}
