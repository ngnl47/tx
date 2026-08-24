package fusion

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ngnl47/tx/meta"
)

// DefaultDirtyTTL 是 Fusion 脏热数据等待 Mongo 确认回写时的默认安全 TTL。
const DefaultDirtyTTL = 30 * 24 * time.Hour

// Config 配置 Fusion 热数据生命周期。
type Config struct {
	DirtyTTL time.Duration
}

// EffectiveDirtyTTL 返回应用默认值后的脏热数据 TTL。
func (config Config) EffectiveDirtyTTL() time.Duration {
	if config.DirtyTTL == 0 {
		return DefaultDirtyTTL
	}
	return config.DirtyTTL
}

func (config Config) validateDirtyTTL() (time.Duration, error) {
	dirtyTTL := config.EffectiveDirtyTTL()
	if dirtyTTL <= 0 {
		return 0, fmt.Errorf("fusion: dirty TTL must be greater than 0")
	}
	if dirtyTTL < time.Second {
		return 0, fmt.Errorf("fusion: dirty TTL must be at least 1s")
	}
	return dirtyTTL, nil
}

// Validate 在 Fusion 创建外部资源前校验脏热数据 TTL 与所有持久化类型的时间窗口。
func (config Config) Validate(types TypeRegistry) error {
	dirtyTTL, err := config.validateDirtyTTL()
	if err != nil {
		return err
	}
	if isNilFusionDependency(types) {
		return fmt.Errorf("fusion: type registry is required")
	}
	specs := append([]meta.TypeSpec(nil), types.TypeSpecs()...)
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Type.Ref.FullName < specs[j].Type.Ref.FullName
	})
	var result error
	for _, spec := range specs {
		result = errors.Join(result, validateDirtyTTLForType(dirtyTTL, spec))
	}
	return result
}

func validateDirtyTTLForType(dirtyTTL time.Duration, spec meta.TypeSpec) error {
	if spec.Storage.Mode != meta.StorageModeFusion {
		return nil
	}
	var result error
	delayDelete := spec.Storage.DelayDelete.Duration
	if delaySave := spec.Storage.DelaySave.Duration; dirtyTTL <= delaySave {
		result = errors.Join(result, fmt.Errorf(
			"fusion: dirty TTL %s must be greater than %s delaySaveTime %s",
			dirtyTTL,
			typeSpecName(spec),
			delaySave,
		))
	}
	if dirtyTTL <= delayDelete {
		result = errors.Join(result, fmt.Errorf(
			"fusion: dirty TTL %s must be greater than %s delayDelTime %s",
			dirtyTTL,
			typeSpecName(spec),
			delayDelete,
		))
	}
	return result
}

func typeSpecName(spec meta.TypeSpec) string {
	if spec.Type.Ref.FullName != "" {
		return spec.Type.Ref.FullName
	}
	if spec.Type.Ref.Name != "" {
		return spec.Type.Ref.Name
	}
	return "<unknown>"
}
