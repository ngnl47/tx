package storage

import (
	"testing"
	"time"
)

func TestValidateExpiringTTLRequiresWholePositiveMilliseconds(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{name: "zero", ttl: 0, wantErr: true},
		{name: "negative", ttl: -time.Second, wantErr: true},
		{name: "below millisecond", ttl: 500 * time.Microsecond, wantErr: true},
		{name: "fractional millisecond", ttl: time.Millisecond + time.Nanosecond, wantErr: true},
		{name: "one millisecond", ttl: time.Millisecond},
		{name: "duration", ttl: 1500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExpiringTTL(tt.ttl)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateExpiringTTL(%s) error = %v, wantErr %v", tt.ttl, err, tt.wantErr)
			}
		})
	}
}

func TestValidateMillisecondDurationRequiresWholePositiveMilliseconds(t *testing.T) {
	for _, value := range []time.Duration{0, -time.Second, time.Nanosecond, time.Millisecond + time.Nanosecond} {
		if err := ValidateMillisecondDuration(value); err == nil {
			t.Fatalf("ValidateMillisecondDuration(%s) error = nil", value)
		}
	}
	for _, value := range []time.Duration{time.Millisecond, 1500 * time.Millisecond} {
		if err := ValidateMillisecondDuration(value); err != nil {
			t.Fatalf("ValidateMillisecondDuration(%s) error = %v", value, err)
		}
	}
}
