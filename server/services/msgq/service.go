// Package msgq provides the standard client-session mailbox service adapter.
package msgq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txserver "github.com/ngnl47/tx/server"
	"github.com/ngnl47/tx/server/adapters/msgqredis"
	"google.golang.org/grpc"
)

const ServiceName = "msgq"

type Options struct {
	InstanceID         string
	Store              txmsgq.SessionStore
	SessionStore       msgqredis.Options
	MaxBacklogMessages int
	MaxBacklogBytes    int
	OfflineTimeout     time.Duration
	SweepInterval      time.Duration
}

// OptionsProvider converts project configuration when msgq is selected.
type OptionsProvider func() (Options, error)

type Service struct {
	options  Options
	provider OptionsProvider

	mu     sync.RWMutex
	client txmsgq.SessionClient
}

func New(options Options) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	return &Service{options: options}, nil
}

// NewConfigured defers project configuration conversion until the service is
// selected for this process. Unselected services therefore cannot block an
// otherwise valid process startup.
func NewConfigured(provider OptionsProvider) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("msgq service: options provider is nil")
	}
	return &Service{provider: provider}, nil
}

func (service *Service) Spec() txserver.Spec {
	return txserver.Spec{
		Name:     ServiceName,
		Requires: txserver.CapabilityStorage | txserver.CapabilityRPC,
	}
}

func (service *Service) Start(runtime txserver.Context) error {
	if runtime == nil || runtime.Tasks() == nil || runtime.RPC() == nil {
		return fmt.Errorf("msgq service: lifecycle and RPC host are required")
	}
	if err := service.configure(); err != nil {
		return err
	}
	instanceID := strings.TrimSpace(service.options.InstanceID)
	if instanceID == "" {
		instanceID = runtime.Instance().ID
	}
	store := service.options.Store
	var err error
	if store == nil {
		advanced := runtime.Advanced()
		if advanced == nil || advanced.Redis() == nil {
			return fmt.Errorf("msgq service: Redis routes are unavailable")
		}
		store, err = msgqredis.New(advanced.Redis(), service.options.SessionStore)
		if err != nil {
			return err
		}
	}
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{
		InstanceID:         instanceID,
		Store:              store,
		MaxBacklogMessages: service.options.MaxBacklogMessages,
		MaxBacklogBytes:    service.options.MaxBacklogBytes,
		OfflineTimeout:     service.options.OfflineTimeout,
		SweepInterval:      service.options.SweepInterval,
	})
	if err != nil {
		return err
	}
	client, err := txmsgq.NewLocalSessionClient(mailbox)
	if err != nil {
		return err
	}
	if err := runtime.RPC().Register(func(registrar grpc.ServiceRegistrar) error {
		return txmsgq.RegisterMailboxService(registrar, mailbox)
	}); err != nil {
		return err
	}
	if err := mailbox.Start(runtime.Tasks()); err != nil {
		return err
	}
	service.mu.Lock()
	service.client = client
	service.mu.Unlock()
	txlog.Info("msgq service initialized", "endpoint", runtime.RPC().Endpoint(), "maxBacklogMessages", service.options.MaxBacklogMessages, "maxBacklogBytes", service.options.MaxBacklogBytes)
	return nil
}

func (service *Service) configure() error {
	if service == nil {
		return fmt.Errorf("msgq service: service is nil")
	}
	if service.provider == nil {
		return validateOptions(service.options)
	}
	options, err := service.provider()
	if err != nil {
		return fmt.Errorf("msgq service: configure: %w", err)
	}
	if err := validateOptions(options); err != nil {
		return err
	}
	service.options = options
	return nil
}

func (service *Service) Stop(context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.client = nil
	service.mu.Unlock()
	return nil
}

func (service *Service) SessionClient() (txmsgq.SessionClient, error) {
	if service == nil {
		return nil, fmt.Errorf("msgq service: local session client is unavailable")
	}
	service.mu.RLock()
	client := service.client
	service.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("msgq service: local session client is unavailable")
	}
	return client, nil
}

func validateOptions(options Options) error {
	if options.Store == nil && (strings.TrimSpace(options.SessionStore.RouteType) == "" || strings.TrimSpace(options.SessionStore.Prefix) == "" || options.SessionStore.TTL <= 0) {
		return fmt.Errorf("msgq service: session store is not configured")
	}
	if options.MaxBacklogMessages <= 0 || options.MaxBacklogBytes < 0 || options.OfflineTimeout < 0 || options.SweepInterval < 0 {
		return fmt.Errorf("msgq service: invalid options")
	}
	return nil
}
