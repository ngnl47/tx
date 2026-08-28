package agent

import "testing"

func TestValidateWebSocketOptionsAcceptsExactOrigins(t *testing.T) {
	t.Parallel()

	options := WebSocketOptions{AllowedOrigins: []string{
		"http://127.0.0.1:5173",
		"https://debug.example.com",
	}}
	if err := validateWebSocketOptions(options); err != nil {
		t.Fatalf("validate exact origins: %v", err)
	}
}

func TestValidateWebSocketOptionsRejectsWildcardAndIncompleteOrigins(t *testing.T) {
	t.Parallel()

	tests := []string{
		"*",
		"https://*.example.com",
		"127.0.0.1:5173",
		"ftp://debug.example.com",
		"https://debug.example.com/path",
	}
	for _, origin := range tests {
		origin := origin
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			if err := validateWebSocketOptions(WebSocketOptions{AllowedOrigins: []string{origin}}); err == nil {
				t.Fatalf("expected origin %q to be rejected", origin)
			}
		})
	}
}
