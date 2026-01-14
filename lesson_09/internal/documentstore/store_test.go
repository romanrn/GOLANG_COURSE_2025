package documentstore

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestSetLogger(t *testing.T) {
	// Should not panic and should accept non-nil
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	SetLogger(l)
	// Passing nil should be a no-op
	SetLogger(nil)
}

func TestNewStore(t *testing.T) {
	s := NewStore()
	if s == nil {
		t.Fatalf("NewStore returned nil")
	}
	if len(s.collections) != 0 {
		t.Fatalf("expected empty collections, got %d", len(s.collections))
	}
}

func TestCreateCollection(t *testing.T) {
	s := NewStore()

	t.Run("invalid name", func(t *testing.T) {
		_, err := s.CreateCollection("   ", &CollectionConfig{PrimaryKey: "id"})
		if err == nil {
			t.Fatalf("expected error for empty name")
		}
	})

	t.Run("nil config", func(t *testing.T) {
		_, err := s.CreateCollection("users", nil)
		if err == nil {
			t.Fatalf("expected error for nil config")
		}
	})

	t.Run("empty primary key", func(t *testing.T) {
		_, err := s.CreateCollection("users", &CollectionConfig{PrimaryKey: ""})
		if err == nil {
			t.Fatalf("expected error for empty primary key")
		}
	})

	t.Run("success_зrimary_key_id", func(t *testing.T) {
		c, err := s.CreateCollection("users", &CollectionConfig{PrimaryKey: "id"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c == nil {
			t.Fatalf("collection is nil")
		}
	})

	t.Run("success_зrimary_key_name", func(t *testing.T) {
		c, err := s.CreateCollection("users2", &CollectionConfig{PrimaryKey: "name"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c == nil {
			t.Fatalf("collection is nil")
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		_, _ = s.CreateCollection("dups", &CollectionConfig{PrimaryKey: "id"})
		_, err := s.CreateCollection("dups", &CollectionConfig{PrimaryKey: "id"})
		if err == nil {
			t.Fatalf("expected duplicate error")
		}
	})
}

func TestGetCollection(t *testing.T) {
	s := NewStore()
	_, _ = s.CreateCollection("users", &CollectionConfig{PrimaryKey: "id"})

	t.Run("empty name", func(t *testing.T) {
		_, err := s.GetCollection("  ")
		if err == nil {
			t.Fatalf("expected error for empty name")
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := s.GetCollection("missing")
		if err == nil {
			t.Fatalf("expected ErrCollectionNotFound, got %v", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		c, err := s.GetCollection("users")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c == nil {
			t.Fatalf("nil collection")
		}
	})
}

func TestDeleteCollection(t *testing.T) {
	s := NewStore()
	_, _ = s.CreateCollection("users", &CollectionConfig{PrimaryKey: "id"})

	t.Run("empty name", func(t *testing.T) {
		if err := s.DeleteCollection("   "); err == nil {
			t.Fatalf("expected error for empty name")
		}
	})

	t.Run("not found", func(t *testing.T) {
		if err := s.DeleteCollection("missing"); err == nil {
			t.Fatalf("expected ErrCollectionNotFound, got %v", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		if err := s.DeleteCollection("users"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := s.GetCollection("users"); err == nil {
			t.Fatalf("collection should be deleted")
		}
	})
}

func TestDumpAndNewStoreFromDump_RoundTrip(t *testing.T) {
	s := NewStore()
	users, _ := s.CreateCollection("users", &CollectionConfig{PrimaryKey: "id"})

	// Add documents
	users.Put(Document{Fields: map[string]DocumentField{
		"id":   {Type: DocumentFieldTypeString, Value: "u1"},
		"name": {Type: DocumentFieldTypeString, Value: "Alice"},
	}})
	users.Put(Document{Fields: map[string]DocumentField{
		"id":   {Type: DocumentFieldTypeString, Value: "u2"},
		"name": {Type: DocumentFieldTypeString, Value: "Bob"},
	}})

	// Dump
	data, err := s.Dump()
	if err != nil {
		t.Fatalf("Dump error: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("dump is not valid JSON")
	}

	// Load
	s2, err := NewStoreFromDump(data)
	if err != nil {
		t.Fatalf("NewStoreFromDump error: %v", err)
	}

	// Verify collections exist
	u2, err := s2.GetCollection("users")
	if err != nil {
		t.Fatalf("users not found in restored store: %v", err)
	}

	// Verify doc counts
	if got := len(u2.documents); got != 2 {
		t.Fatalf("expected 2 users, got %d", got)
	}
}

func TestNewStoreFromDump_Errors(t *testing.T) {
	t.Run("empty dump", func(t *testing.T) {
		_, err := NewStoreFromDump(nil)
		if err == nil {
			t.Fatalf("expected error for empty dump")
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := NewStoreFromDump([]byte("{"))
		if err == nil {
			t.Fatalf("expected JSON error")
		}
	})

	t.Run("invalid collection in dump", func(t *testing.T) {
		// Invalid because PrimaryKey is empty
		bad := dumpStore{
			Collections: map[string]dumpCollection{
				"bad": {Config: CollectionConfig{PrimaryKey: ""}, Documents: nil},
			},
		}
		data, _ := json.Marshal(bad)
		_, err := NewStoreFromDump(data)
		if err == nil {
			t.Fatalf("expected error for invalid collection config")
		}
	})
}

func TestDumpToFileAndNewStoreFromFile(t *testing.T) {
	s := NewStore()
	users, _ := s.CreateCollection("users", &CollectionConfig{PrimaryKey: "id"})
	users.Put(Document{Fields: map[string]DocumentField{
		"id":   {Type: DocumentFieldTypeString, Value: "u1"},
		"name": {Type: DocumentFieldTypeString, Value: "Alice"},
	}})

	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")

	// Dump to file
	if err := s.DumpToFile(path); err != nil {
		t.Fatalf("DumpToFile error: %v", err)
	}

	// New store from file
	s2, err := NewStoreFromFile(path)
	if err != nil {
		t.Fatalf("NewStoreFromFile error: %v", err)
	}

	// Validate content
	u2, err := s2.GetCollection("users")
	if err != nil {
		t.Fatalf("users not found: %v", err)
	}
	if len(u2.documents) != 1 {
		t.Fatalf("expected 1 user, got %d", len(u2.documents))
	}
}

func TestFileAPIs_Errors(t *testing.T) {
	t.Run("NewStoreFromFile empty name", func(t *testing.T) {
		_, err := NewStoreFromFile("   ")
		if err == nil {
			t.Fatalf("expected error for empty filename")
		}
	})

	t.Run("NewStoreFromFile missing file", func(t *testing.T) {
		_, err := NewStoreFromFile(filepath.Join(t.TempDir(), "missing.json"))
		if err == nil {
			t.Fatalf("expected error for missing file")
		}
	})

	t.Run("DumpToFile empty name", func(t *testing.T) {
		s := NewStore()
		if err := s.DumpToFile("   "); err == nil {
			t.Fatalf("expected error for empty filename")
		}
	})

	t.Run("DumpToFile write error", func(t *testing.T) {
		s := NewStore()
		// Use an invalid path to trigger write error. On Unix, writing to a directory path should fail.
		if err := s.DumpToFile(t.TempDir()); err == nil {
			t.Fatalf("expected write error")
		}
	})
}
