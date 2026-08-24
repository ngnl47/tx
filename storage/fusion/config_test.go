package fusion

import (
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
)

func TestConfigValidatesDirtyTTLAgainstFusionTypeLifetimes(t *testing.T) {
	registry := meta.NewRegistry()
	for _, spec := range []meta.TypeSpec{
		{
			Type: meta.Type{Ref: meta.TypeRef{FullName: "data.GDRole"}},
			Storage: meta.StorageSpec{
				Mode:        meta.StorageModeFusion,
				DelaySave:   meta.Duration{Duration: 10 * time.Minute},
				DelayDelete: meta.Duration{Duration: 24 * time.Hour},
			},
		},
		{
			Type: meta.Type{Ref: meta.TypeRef{FullName: "data.GDRank"}},
			Storage: meta.StorageSpec{
				Mode:        meta.StorageModeFusion,
				DelaySave:   meta.Duration{Duration: 48 * time.Hour},
				DelayDelete: meta.Duration{Duration: 12 * time.Hour},
			},
		},
		{
			Type:    meta.Type{Ref: meta.TypeRef{FullName: "data.GDRedisOnly"}},
			Storage: meta.StorageSpec{Mode: meta.StorageModeRedis, DelayDelete: meta.Duration{Duration: 90 * 24 * time.Hour}},
		},
	} {
		if err := registry.RegisterType(spec); err != nil {
			t.Fatalf("RegisterType(%s) returned error: %v", spec.Type.Ref.FullName, err)
		}
	}

	tests := []struct {
		name     string
		dirtyTTL time.Duration
		want     []string
	}{
		{name: "valid", dirtyTTL: 30 * 24 * time.Hour},
		{name: "equal delay delete", dirtyTTL: 24 * time.Hour, want: []string{"data.GDRole", "delayDelTime", "24h0m0s"}},
		{name: "less than delay save", dirtyTTL: 36 * time.Hour, want: []string{"data.GDRank", "delaySaveTime", "48h0m0s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (Config{DirtyTTL: tt.dirtyTTL}).Validate(registry)
			if len(tt.want) == 0 {
				if err != nil {
					t.Fatalf("Validate returned error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate error = nil, want invalid dirty TTL")
			}
			for _, fragment := range tt.want {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("Validate error = %q, want fragment %q", err, fragment)
				}
			}
		})
	}
}

func TestConfigUsesThirtyDayDefaultDirtyTTL(t *testing.T) {
	config := Config{}
	if got := config.EffectiveDirtyTTL(); got != 30*24*time.Hour {
		t.Fatalf("EffectiveDirtyTTL = %s, want 720h", got)
	}
}

func TestConfigRejectsDirtyTTLBelowRedisMillisecondPrecision(t *testing.T) {
	registry := meta.NewRegistry()
	err := (Config{DirtyTTL: 500 * time.Microsecond}).Validate(registry)
	if err == nil || !strings.Contains(err.Error(), "at least 1ms") {
		t.Fatalf("Validate error = %v, want Redis millisecond precision error", err)
	}
}
