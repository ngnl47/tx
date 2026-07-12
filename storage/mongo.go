package storage

import "fmt"

func (cfg MongoConfig) Filter(values map[string]any) (map[string]any, error) {
	id, ok := values[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("%w: mongo id %s", ErrMissingValue, cfg.ID)
	}
	filter := map[string]any{cfg.ID: id}
	if !cfg.Sharding.Enabled {
		return filter, nil
	}
	shardBy := cfg.Route.By
	if shardBy == "" || shardBy == cfg.ID {
		return filter, nil
	}
	shardValue, ok := values[shardBy]
	if !ok {
		return nil, fmt.Errorf("%w: mongo shard %s", ErrMissingValue, shardBy)
	}
	filter[shardBy] = shardValue
	return filter, nil
}
