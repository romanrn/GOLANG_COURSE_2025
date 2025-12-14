package documentstore

import (
	"fmt"
	"strings"
	"sync"
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

func (s *Store) CreateCollection(name string, cfg *CollectionConfig) (bool, *Collection) {
	// Створюємо нову колекцію і повертаємо `true` якщо колекція була створена
	// Якщо ж колекція вже створеня то повертаємо `false` та nil
	name = strings.TrimSpace(name)
	if name == "" || cfg == nil || cfg.PrimaryKey == "" {
		fmt.Println("[Store] Error: invalid collection name or config")
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		return false, nil
	}

	collection := NewCollection(cfg)

	s.collections[name] = collection
	return true, collection

}

func (s *Store) GetCollection(name string) (*Collection, bool) {
	if strings.TrimSpace(name) == "" {
		fmt.Println("[Store GetCollection] Error: name is empty")
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	collection, exists := s.collections[name]
	if !exists {
		fmt.Printf("\n %s collection not found\n", name)
		return nil, false
	}

	return collection, exists
}

func (s *Store) DeleteCollection(name string) bool {
	if strings.TrimSpace(name) == "" {
		fmt.Println("[Store DeleteCollection Delete] Error: name is empty")
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.collections[name]; exists {
		fmt.Printf("[Store DeleteCollection Delete]  %s collection\n", name)
		delete(s.collections, name)
		return true
	}
	fmt.Printf("[Store DeleteCollection Delete]  %s collection doesn't exist\n", name)
	return false
}
