package control

import (
	"context"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/storage"
	"google.golang.org/grpc"
)

// StorageIOSnapshot 将累计统计与进程实例身份一起返回，禁止跨实例相减。
type StorageIOSnapshot struct {
	InstanceID string
	Stats      []storage.IOStat
}

// StorageIOProvider 由宿主提供内存快照，不触发外部存储读取或清零。
type StorageIOProvider interface{ StorageIOStats() StorageIOSnapshot }

// RegisterStorageIOService 注册只读存储统计服务。
func RegisterStorageIOService(registrar grpc.ServiceRegistrar, provider StorageIOProvider) error {
	if registrar == nil || provider == nil {
		return ErrControllerNil
	}
	controlpb.RegisterStorageIOServiceServer(registrar, &storageIOService{provider: provider})
	return nil
}

type storageIOService struct {
	controlpb.UnimplementedStorageIOServiceServer
	provider StorageIOProvider
}

func (s *storageIOService) Snapshot(context.Context, *controlpb.Empty) (*controlpb.StorageIOSnapshot, error) {
	value := s.provider.StorageIOStats()
	result := &controlpb.StorageIOSnapshot{InstanceId: value.InstanceID}
	for _, stat := range value.Stats {
		result.Stats = append(result.Stats, &controlpb.StorageIOStat{Backend: stat.Backend, Operation: stat.Operation,
			Calls: stat.Calls, Commands: stat.Commands, Errors: stat.Errors,
			TotalDurationNs: int64(stat.TotalDuration), MaxDurationNs: int64(stat.MaxDuration)})
	}
	return result, nil
}
