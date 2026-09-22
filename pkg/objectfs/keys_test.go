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

// I5's regression guard at the unit level: hashKey must be stable across
// calls (the same key always yields the same inode number, so go-fuse's
// addNewChild recognizes a repeat lookup as the same inode instead of
// minting a fresh one) and must actually vary across distinct keys (a
// constant would "pass" the stability check while corrupting every distinct
// file onto one inode).
func TestHashKeyStableAndDistinct(t *testing.T) {
	const key = "session.key"
	first := hashKey(key)
	second := hashKey(key)
	if first != second {
		t.Fatalf("hashKey(%q) = %d then %d; not stable across calls", key, first, second)
	}
	if hashKey("a") == hashKey("b") {
		t.Fatal("hashKey collided on two distinct one-byte keys — pick better test keys or fix the hash")
	}
}
