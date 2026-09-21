package containers

import "testing"

// Trimmed from a real Docker 29.6 reply: one running, one exited, and an
// unrelated field (Command) that must never be decoded
const sampleList = `[
  {
    "Id": "2e303154764f2e04bbaacca18140c33112465ffdf1814c8aed8e12f90c986d62",
    "Names": ["/valkey-valkey-1"],
    "Image": "valkey/valkey:8-alpine",
    "Command": "valkey-server --requirepass secret",
    "State": "running",
    "Status": "Up 4 hours (healthy)"
  },
  {
    "Id": "6a415bcfdc94aa0e1f5b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef",
    "Names": ["/postgres-postgres-1"],
    "Image": "postgres:16",
    "State": "exited",
    "Status": "Exited (137) 3 minutes ago"
  }
]`

func TestParse(t *testing.T) {
	got, err := Parse([]byte(sampleList))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	want := Entry{
		ID:    "2e303154764f2e04bbaacca18140c33112465ffdf1814c8aed8e12f90c986d62",
		Names: []string{"/valkey-valkey-1"},
		Image: "valkey/valkey:8-alpine",
		State: "running",
	}
	if got[0].ID != want.ID || got[0].Names[0] != want.Names[0] || got[0].Image != want.Image || got[0].State != want.State {
		t.Fatalf("expected %+v, got %+v", want, got[0])
	}
}

func TestParse_Empty(t *testing.T) {
	got, err := Parse([]byte(`[]`))
	if err != nil || len(got) != 0 {
		t.Fatalf("expected no entries and no error, got %v, %v", got, err)
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := Parse([]byte(`{"message":"not a list"}`)); err == nil {
		t.Fatal("expected an error for a reply that is not a list")
	}
}

func TestToContainers_TrimsSlashAndSortsByName(t *testing.T) {
	entries, err := Parse([]byte(sampleList))
	if err != nil {
		t.Fatal(err)
	}
	got := ToContainers(entries)
	if len(got) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(got))
	}
	want := Container{
		ID:    "6a415bcfdc94aa0e1f5b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef",
		Name:  "postgres-postgres-1",
		Image: "postgres:16",
		State: "exited",
	}
	if got[0] != want {
		t.Fatalf("expected %+v first, got %+v", want, got[0])
	}
	if got[1].Name != "valkey-valkey-1" {
		t.Fatalf("expected valkey-valkey-1 second, got %q", got[1].Name)
	}
}

func TestToContainers_NoNames(t *testing.T) {
	got := ToContainers([]Entry{{ID: "abc", State: "created"}})
	if got[0].Name != "" || got[0].ID != "abc" {
		t.Fatalf("expected an empty name and the id kept, got %+v", got[0])
	}
}

// Built non-nil so an empty list publishes [] rather than null
func TestToContainers_EmptyIsNotNil(t *testing.T) {
	if got := ToContainers(nil); got == nil {
		t.Fatal("expected a non-nil empty slice")
	}
}
