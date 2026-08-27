package ant

import (
	"errors"
	"fmt"

	framework "github.com/ngnl47/tx/internal/framework"
	txstorage "github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

// AssemblySelection 是依赖图解析后的 CMod 与 L4 服务只读集合。
type AssemblySelection struct {
	kernel framework.AssemblySelection
}

// NewAssemblySelection 创建供项目配置转换和测试使用的独立启动选择。
func NewAssemblySelection(cmodIDs []CModID, serviceNames []string) AssemblySelection {
	return AssemblySelection{kernel: framework.NewAssemblySelection(toFrameworkCModIDs(cmodIDs), serviceNames)}
}

// HasCMod 判断能力是否进入本轮启动闭包。
func (selection AssemblySelection) HasCMod(id CModID) bool {
	return selection.kernel.HasCMod(toFrameworkCModID(id))
}

// HasService 判断服务是否进入本轮启动集合。
func (selection AssemblySelection) HasService(name string) bool {
	return selection.kernel.HasService(name)
}

// CModIDs 返回稳定拓扑顺序的 Ant CMod ID。
func (selection AssemblySelection) CModIDs() []CModID {
	kernel := selection.kernel.CModIDs()
	result := make([]CModID, len(kernel))
	for i, id := range kernel {
		result[i] = CModID(id)
	}
	return result
}

// ServiceNames 返回稳定字典序的服务名。
func (selection AssemblySelection) ServiceNames() []string {
	return selection.kernel.ServiceNames()
}

// CModConfigProvider 在依赖图确定后惰性转换本轮实际需要的配置。
type CModConfigProvider func(AssemblySelection) (Config, error)

// Builder 增量收集 Ant 配置、CMod、服务和项目绑定。
type Builder struct {
	config         Config
	cmods          []CMod
	services       []ServiceRegistration
	configProvider CModConfigProvider
	types          fusion.TypeRegistry
	bindStorage    func(StorageReaders)
	setups         []func(Context) (func(), error)
	errs           []error
}

// New 创建 Ant Builder。该函数不创建连接或启动 goroutine。
func New(config Config) *Builder {
	return &Builder{config: config}
}

// CMods 注册项目自定义能力模组。
func (builder *Builder) CMods(values ...CMod) *Builder {
	if builder == nil {
		return nil
	}
	builder.cmods = append(builder.cmods, values...)
	return builder
}

// Services 注册可由启动参数选择的 L4 服务。
func (builder *Builder) Services(values ...ServiceRegistration) *Builder {
	if builder == nil {
		return nil
	}
	builder.services = append(builder.services, values...)
	return builder
}

// ConfigProvider 注册按已选依赖闭包惰性转换配置的项目函数。
func (builder *Builder) ConfigProvider(provider CModConfigProvider) *Builder {
	if builder == nil {
		return nil
	}
	if provider == nil {
		builder.errs = append(builder.errs, errors.New("ant: CMod config provider is nil"))
	} else if builder.configProvider != nil {
		builder.errs = append(builder.errs, errors.New("ant: CMod config provider is already registered"))
	} else {
		builder.configProvider = provider
	}
	return builder
}

// GeneratedData 接入 txgen 生成的类型注册表和读取器绑定函数。
func (builder *Builder) GeneratedData(types fusion.TypeRegistry, bind func(StorageReaders)) *Builder {
	if builder == nil {
		return nil
	}
	if types == nil || bind == nil {
		builder.errs = append(builder.errs, errors.New("ant: generated data registry and binding are required"))
	} else {
		builder.types = types
		builder.bindStorage = bind
	}
	return builder
}

// Setup 注册能力启动后、服务构造前执行的项目胶水逻辑。
func (builder *Builder) Setup(setup func(Context) (func(), error)) *Builder {
	if builder == nil {
		return nil
	}
	if setup == nil {
		builder.errs = append(builder.errs, errors.New("ant: setup is nil"))
	} else {
		builder.setups = append(builder.setups, setup)
	}
	return builder
}

// Build 校验静态注册信息并构造 Ant Server，不启动外部资源。
func (builder *Builder) Build() (*Server, error) {
	if builder == nil {
		return nil, errors.New("ant: builder is nil")
	}
	if err := errors.Join(builder.errs...); err != nil {
		return nil, err
	}
	options := make([]framework.Option, 0, 4+len(builder.cmods))
	if builder.configProvider != nil {
		options = append(options, framework.WithCModConfigProvider(func(selection framework.AssemblySelection) (framework.Config, error) {
			config, err := builder.configProvider(AssemblySelection{kernel: selection})
			if err != nil {
				return framework.Config{}, err
			}
			return config.toFramework(), nil
		}))
	}
	if builder.types != nil {
		options = append(options, framework.WithGeneratedData(builder.types, func(readers framework.StorageReaders) {
			builder.bindStorage(adaptStorageReaders(readers))
		}))
	}
	for _, setup := range builder.setups {
		current := setup
		options = append(options, framework.WithSetup(func(runtime framework.Context) (func(), error) {
			return current(newSetupContext(runtime))
		}))
	}
	frameworkServices := make([]framework.Service, 0, len(builder.services))
	for _, registration := range builder.services {
		if err := validateRegistration(registration); err != nil {
			return nil, err
		}
		frameworkServices = append(frameworkServices, &serviceAdapter{registration: registration})
	}
	options = append(options, framework.WithService(frameworkServices...))
	frameworkCMods := make([]framework.CMod, 0, len(builder.cmods))
	for _, mod := range builder.cmods {
		if isNil(mod) {
			return nil, errors.New("ant: CMod is nil")
		}
		frameworkCMods = append(frameworkCMods, &cmodAdapter{mod: mod})
	}
	if len(frameworkCMods) > 0 {
		options = append(options, framework.WithCMod(frameworkCMods...))
	}
	kernel, err := framework.New(builder.config.toFramework(), options...)
	if err != nil {
		return nil, fmt.Errorf("ant: build server: %w", err)
	}
	return &Server{kernel: kernel}, nil
}

type storageReadersAdapter struct {
	kernel framework.StorageReaders
}

func adaptStorageReaders(readers framework.StorageReaders) StorageReaders {
	if readers == nil {
		return nil
	}
	return storageReadersAdapter{kernel: readers}
}

func (adapter storageReadersAdapter) RedisReader() txstorage.DocumentLoader {
	return adapter.kernel.RedisReader()
}

func (adapter storageReadersAdapter) MongoReader() txstorage.DocumentLoader {
	return adapter.kernel.MongoReader()
}

func (adapter storageReadersAdapter) FusionReader() txstorage.DocumentLoader {
	return adapter.kernel.FusionReader()
}
