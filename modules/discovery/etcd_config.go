package discovery

import (
	"errors"
	"strings"
	"time"
)

const defaultEtcdPrefix = "/tx/discovery/instances"

var ErrInvalidEtcdConfig = errors.New("discovery: invalid etcd config")

type EtcdConfig struct {
	Endpoints   []string
	Prefix      string
	LeaseTTL    time.Duration
	DialTimeout time.Duration
}

func (c EtcdConfig) Normalize() (EtcdConfig, error) {
	if len(c.Endpoints) == 0 || c.LeaseTTL < time.Second || c.LeaseTTL%time.Second != 0 {
		return EtcdConfig{}, ErrInvalidEtcdConfig
	}
	c.Endpoints = append([]string(nil), c.Endpoints...)
	for index, endpoint := range c.Endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return EtcdConfig{}, ErrInvalidEtcdConfig
		}
		c.Endpoints[index] = endpoint
	}
	prefix := strings.Trim(strings.TrimSpace(c.Prefix), "/")
	if prefix == "" {
		c.Prefix = defaultEtcdPrefix
	} else if strings.HasSuffix(prefix, "/instances") {
		c.Prefix = "/" + prefix
	} else {
		c.Prefix = "/" + prefix + "/instances"
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	return c, nil
}

func (c EtcdConfig) ValidateInstance(instance Instance) error {
	if err := instance.Validate(); err != nil || strings.Contains(instance.ID, "/") {
		return ErrInvalidInstance
	}
	return nil
}
