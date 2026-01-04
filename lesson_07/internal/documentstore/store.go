package documentstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	ErrCollectionAlreadyExists    = errors.New("collection already exists")
	ErrCollectionInvalidNameOrKey = errors.New("invalid collection name or config")
	ErrCollectionNotFound         = errors.New("collection not found")
	ErrCollectionFileName         = errors.New("file name is not specified")
	ErrStoreDump                  = errors.New("the provided dump is empty or invalid")
	ErrReadStoreDump              = errors.New("the Error reading store dump from file")
)

// Build a default JSON slog logger to stdout
func defaultLogger() *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With(slog.String("component", "documentstore"))
}

// Package-level structured logger initialized via defaultLogger.
var pkgLogger = defaultLogger()

// Allow applications to inject/override the logger if needed.
func SetLogger(l *slog.Logger) {
	if l == nil {
		// keep existing logger if nil passed
		return
	}
	pkgLogger = l
}

type Store struct {
	collections map[string]*Collection
	mu          sync.RWMutex
}

func NewStore() *Store {
	pkgLogger.Info("initializing store")
	return &Store{
		collections: make(map[string]*Collection),
	}
}

// Since Store contains  private vars,  we need to dump collections along with their configs and documents,
// The dump structure is going to be used for serialization.
type dumpCollection struct {
	Config    CollectionConfig `json:"config"`
	Documents []Document       `json:"documents"`
}

type dumpStore struct {
	Collections map[string]dumpCollection `json:"collections"`
}

func (s *Store) CreateCollection(name string, cfg *CollectionConfig) (*Collection, error) {
	// Створюємо нову колекцію і повертаємо `true` якщо колекція була створена
	// Якщо ж колекція вже створення та повертаємо error
	name = strings.TrimSpace(name)
	if name == "" || cfg == nil || cfg.PrimaryKey == "" {
		pkgLogger.Error("[Store] Error: invalid collection name or config", slog.String("name", name))
		return nil, ErrCollectionInvalidNameOrKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		pkgLogger.Warn("collection already exists", slog.String("name", name))
		return nil, ErrCollectionAlreadyExists
	}

	collection := NewCollection(cfg)
	pkgLogger.Info("collection created", slog.String("name", name), slog.String("primaryKey", cfg.PrimaryKey))

	s.collections[name] = collection
	return collection, nil
}

func (s *Store) GetCollection(name string) (*Collection, error) {
	if strings.TrimSpace(name) == "" {
		pkgLogger.Error("[Store GetCollection] collection name is empty")
		return nil, ErrCollectionInvalidNameOrKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	collection, exists := s.collections[name]
	if !exists {
		pkgLogger.Error("collection not found", slog.String("name", name))
		return nil, ErrCollectionNotFound
	}
	pkgLogger.Info("collection returned", slog.String("name", name))
	return collection, nil
}

func (s *Store) DeleteCollection(name string) error {
	if strings.TrimSpace(name) == "" {
		pkgLogger.Error("[Store DeleteCollection Delete] collection name is empty")
		return ErrCollectionInvalidNameOrKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		pkgLogger.Info("[Store DeleteCollection Delete] deleting collection", slog.String("name", name))
		delete(s.collections, name)
		return nil
	}
	pkgLogger.Error("[Store DeleteCollection Delete] collection doesn't exist", slog.String("name", name))
	return ErrCollectionNotFound
}

// lesson_06

func NewStoreFromDump(dump []byte) (*Store, error) {
	// Функція повинна створити та проініціалізувати новий `Store`
	// зі всіма колекціями да даними з вхідного дампу.

	// Implementation
	if len(dump) == 0 {
		pkgLogger.Error("dump is empty")
		return nil, ErrStoreDump
	}
	var ds dumpStore
	if err := json.Unmarshal(dump, &ds); err != nil {
		pkgLogger.Error("failed to unmarshal dump", slog.Any("error", err))
		return nil, err
	}
	store := NewStore()
	for name, collDump := range ds.Collections {
		collection, err := store.CreateCollection(name, &collDump.Config)
		if err != nil {
			pkgLogger.Error("failed to create collection from dump", slog.String("name", name), slog.Any("error", err))
			return nil, fmt.Errorf("failed to create collection '%s': %w", name, err)
		}
		for _, doc := range collDump.Documents {
			if collection.Put(doc) != nil {
				pkgLogger.Error("failed to put document into collection from dump", slog.String("collection", name), slog.Any("document", doc))
				return nil, fmt.Errorf("failed to put document into collection '%s' from dump", name)
			}
		}
		pkgLogger.Info("loaded collection from dump", slog.String("name", name), slog.Int("documents", len(collDump.Documents)))
	}
	pkgLogger.Info("store initialized from dump", slog.Int("collections", len(store.collections)))
	return store, nil
}

func (s *Store) Dump() ([]byte, error) {
	// Методи повинен віддати дамп нашого стору в який включені дані про колекції та документ

	// TODO: Implement
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds := dumpStore{Collections: make(map[string]dumpCollection)}
	for name, coll := range s.collections {
		// Read collection content under its read lock
		coll.mu.RLock()
		docs := make([]Document, 0, len(coll.documents))
		for _, doc := range coll.documents {
			if doc != nil {
				docs = append(docs, *doc)
			}
		}
		cfg := coll.cfg
		coll.mu.RUnlock()
		ds.Collections[name] = dumpCollection{Config: cfg, Documents: docs}
		pkgLogger.Info("prepared collection for dump", slog.String("name", name), slog.Int("documents", len(docs)))
	}
	data, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		pkgLogger.Error("failed to marshal store dump", slog.Any("error", err))
		return nil, fmt.Errorf("failed to marshal store dump: %w", err)
	}
	pkgLogger.Info("store dump generated", slog.Int("bytes", len(data)), slog.Int("collections", len(s.collections)))
	return data, nil
}

// Значення яке повертає метод `store.Dump()` має без помилок оброблятись функцією `NewStoreFromDump`

func NewStoreFromFile(filename string) (*Store, error) {
	filename = strings.TrimSpace(filename)
	// Робить те ж саме що і функція `NewStoreFromDump`, але сам дамп має діставатись з файлу
	// TODO: Implement
	if filename == "" {
		pkgLogger.Error("filename is empty")
		return nil, ErrCollectionFileName
	}
	fileByte, err := os.ReadFile(filename)
	if err != nil {
		pkgLogger.Error("failed to read store dump file", slog.String("file", filename), slog.Any("error", err))
		return nil, fmt.Errorf("%w: %v", ErrReadStoreDump, err)
	}
	pkgLogger.Info("read dump file", slog.String("file", filename), slog.Int("bytes", len(fileByte)))
	return NewStoreFromDump(fileByte)
}

func (s *Store) DumpToFile(filename string) error {
	// Робить те ж саме що і метод  `Dump`, але записує у файл замість того щоб повертати сам дамп
	// TODO: Implement
	// https://pkg.go.dev/os@go1.25.5#WriteFile
	filename = strings.TrimSpace(filename)
	if filename == "" {
		pkgLogger.Error("filename is empty")
		return ErrCollectionFileName
	}
	dump, err := s.Dump()
	if err != nil {
		pkgLogger.Error("failed to generate dump", slog.Any("error", err))
		return err
	}
	if err := os.WriteFile(filename, dump, 0644); err != nil {
		pkgLogger.Error("failed to write dump to file", slog.String("file", filename), slog.Any("error", err))
		return err
	}
	pkgLogger.Info("dump written to file", slog.String("file", filename), slog.Int("bytes", len(dump)))
	return nil
}
