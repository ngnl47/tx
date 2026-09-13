package actortrial

import "github.com/ngnl47/tx/internal/actorhost"

// 试验与基准继续使用原名称，所有行为来自私有宿主；正式代码不引用本试验包。
type Runtime = actorhost.Runtime
type Identity = actorhost.Identity
type Target = actorhost.Target
type Context = actorhost.Context
type Behavior = actorhost.Behavior
type Future = actorhost.Future
type RemoteError = actorhost.RemoteError
type Placement = actorhost.Placement
type ActivationState = actorhost.ActivationState
type ActivationStatus = actorhost.ActivationStatus
type ActivationLimits = actorhost.ActivationLimits
type ActivationUsage = actorhost.ActivationUsage

var (
	New                 = actorhost.New
	NewFuture           = actorhost.NewFuture
	NewPlacement        = actorhost.NewPlacement
	remoteError         = actorhost.RemoteErrorFrom
	ErrActivationDenied = actorhost.ErrActivationDenied
	ErrDeactivated      = actorhost.ErrDeactivated
	ErrMailboxFull      = actorhost.ErrMailboxFull
	ErrDraining         = actorhost.ErrDraining
	ErrPanicked         = actorhost.ErrPanicked
	ErrNotOwner         = actorhost.ErrNotOwner
	ErrCapacity         = actorhost.ErrCapacity
)

const (
	Unloaded     = actorhost.Unloaded
	Initializing = actorhost.Initializing
	Ready        = actorhost.Ready
	Closing      = actorhost.Closing
	Failed       = actorhost.Failed
)
