package objectfs

import (
	"testing"
	"time"
)

func newSnap(data map[string][]byte) *Snapshot {
	return &Snapshot{Data: data, ResourceVersion: "42", FetchedAt: time.Unix(0, 0)}
}

func TestSnapshotGet(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one")})

	got, ok := s.Get("a")
	if !ok || string(got) != "one" {
		t.Fatalf("Get(a) = %q, %v; want \"one\", true", got, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error("Get(missing) reported ok")
	}
}

func TestSnapshotGetIsACopy(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one")})

	got, _ := s.Get("a")
	got[0] = 'X'

	again, _ := s.Get("a")
	if string(again) != "one" {
		t.Fatalf("snapshot mutated through returned slice: %q", again)
	}
}

func TestSnapshotKeysSorted(t *testing.T) {
	s := newSnap(map[string][]byte{"c": {}, "a": {}, "b": {}})

	want := []string{"a", "b", "c"}
	got := s.Keys()
	if len(got) != len(want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys() = %v, want %v", got, want)
		}
	}
}

func TestSnapshotSize(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("12345"), "b": []byte("123")})
	if got := s.Size(); got != 8 {
		t.Fatalf("Size() = %d, want 8", got)
	}
}

func TestSnapshotWith(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one"), "b": []byte("two")})

	next := s.With(map[string][]byte{"a": []byte("ONE")}, []string{"b"})

	if got, _ := next.Get("a"); string(got) != "ONE" {
		t.Errorf("a = %q, want ONE", got)
	}
	if _, ok := next.Get("b"); ok {
		t.Error("b should have been deleted")
	}
	if got, _ := s.Get("a"); string(got) != "one" {
		t.Errorf("original mutated: a = %q", got)
	}
	// ResourceVersion must be cleared: With's result does not correspond to
	// any version the API server has seen, and Cache.Commit relies on that
	// to route this snapshot through the unconditional Set rather than
	// setFromWatch's ResourceVersion-ordering guard (which would otherwise
	// compare against an empty string via strconv.ParseUint, fail to parse,
	// and — while currently falling safely open — is not the code path an
	// empty RV is supposed to travel at all).
	if next.ResourceVersion != "" {
		t.Errorf("ResourceVersion = %q, want empty", next.ResourceVersion)
	}
}
