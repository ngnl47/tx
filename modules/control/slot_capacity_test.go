package control

import (
	"fmt"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSlotCapacityStatus(t *testing.T) {
	err := fmt.Errorf("容量预检: %w", discovery.ErrSlotCapacityUnavailable)
	for _, got := range []error{slotCreationStatusFromError(err), slotStatusFromError(err)} {
		if status.Code(got) != codes.ResourceExhausted {
			t.Fatalf("容量不足必须与内部错误区分: %v", got)
		}
	}
}
