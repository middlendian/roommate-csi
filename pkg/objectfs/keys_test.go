package objectfs

import "testing"

func TestValidKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"simple", "config.json", true},
		{"leading dot", ".credentials.json", true},
		{"underscore", "MY_TOKEN", true},
		{"hyphen", "session-key", true},
		{"digits", "key123", true},
		{"empty", "", false},
		{"dot", ".", false},
		{"dotdot", "..", false},
		{"slash", "a/b", false},
		{"space", "my key", false},
		{"colon", "a:b", false},
		{"newline", "a\nb", false},
		{"too long", string(make([]byte, 254)), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidKey(tt.key); got != tt.want {
				t.Errorf("ValidKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}
