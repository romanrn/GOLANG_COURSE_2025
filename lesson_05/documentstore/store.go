package documentstore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrCollectionAlreadyExists    = errors.New("collection already exists")
	ErrCollectionInvalidNameOrKey = errors.New("invalid collection name or config")
	ErrCollectionNotFound         = errors.New("collection not found")
)

type Store struct {
	collections map[string]*Collection
	mu          sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		collections: make(map[string]*Collection),
	}
}

func (s *Store) CreateCollection(name string, cfg *CollectionConfig) (*Collection, error) {
	// Створюємо нову колекцію і повертаємо `true` якщо колекція була створена
	// Якщо ж колекція вже створення та повертаємо error
	name = strings.TrimSpace(name)
	if name == "" || cfg == nil || cfg.PrimaryKey == "" {
		fmt.Println("[Store] Error: invalid collection name or config")
		return nil, ErrCollectionInvalidNameOrKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		return nil, ErrCollectionAlreadyExists
	}

	collection := NewCollection(cfg)

	s.collections[name] = collection
	return collection, nil

}

func (s *Store) GetCollection(name string) (*Collection, error) {
	if strings.TrimSpace(name) == "" {
		fmt.Println("[Store GetCollection] Error: name is empty")
		return nil, ErrCollectionInvalidNameOrKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	collection, exists := s.collections[name]
	if !exists {
		fmt.Printf("\n %s collection not found\n", name)
		return nil, ErrCollectionNotFound
	}

	return collection, nil
}

func (s *Store) DeleteCollection(name string) error {
	if strings.TrimSpace(name) == "" {
		fmt.Println("[Store DeleteCollection Delete] Error: name is empty")
		return ErrCollectionInvalidNameOrKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		fmt.Printf("[Store DeleteCollection Delete]  %s collection\n", name)
		delete(s.collections, name)
		return nil
	}
	fmt.Printf("[Store DeleteCollection Delete]  %s collection doesn't exist\n", name)
	return ErrCollectionNotFound
}
