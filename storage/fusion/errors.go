package fusion

import "errors"

var (
	ErrTaskBackendNotConfigured           = errors.New("fusion: delayed task backend is not configured")
	ErrTaskHandlerNotConfigured           = errors.New("fusion: delayed task handler is not configured")
	ErrSchedulerContextNotConfigured      = errors.New("fusion: scheduler lifecycle context is not configured")
	ErrDelayTaskDependenciesNotConfigured = errors.New("fusion: delayed task dependencies are not configured")
	ErrStrategyDependenciesNotConfigured  = errors.New("fusion: strategy dependencies are not configured")
	ErrRedisNotConfigured                 = errors.New("fusion: redis change applier is not configured")
	ErrMongoQueueNotConfigured            = errors.New("fusion: mongo delayed queue is not configured")
	ErrHandoffStoreNotConfigured          = errors.New("fusion: handoff store is not configured")
	ErrHandoffTypesNotConfigured          = errors.New("fusion: handoff type registry is not configured")
)
