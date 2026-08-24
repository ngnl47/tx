package server

import "errors"

func (server *Server) delaySchedulerCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModDelayScheduler, Requires: []CModID{CModFusionStorage}},
		validate: func(CModContext) error {
			return server.config.Runtime.Scheduler.validate()
		},
		start: func(CModContext) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return errors.New("storage runtime is not configured")
			}
			if err := server.runtime.storage.prepareDelayScheduler(server.config.Runtime.storageOptions()); err != nil {
				return err
			}
			return server.runtime.storage.startDelayScheduler(server.runtime.lifecycle)
		},
	}
}
