package documentstore

import (
	"fmt"
	"strings"
	"sync"
)

type Collection struct {
	cfg       CollectionConfig
	documents map[string]*Document
	mu        sync.RWMutex
}

type CollectionConfig struct {
	PrimaryKey string
}

func NewCollection(cfg *CollectionConfig) *Collection {
	defaultCfg := CollectionConfig{
		PrimaryKey: "id",
	}
	if cfg != nil && strings.TrimSpace(cfg.PrimaryKey) != "" {
		defaultCfg = *cfg
	}

	return &Collection{
		cfg:       defaultCfg,
		documents: make(map[string]*Document),
	}
}

func (s *Collection) Put(doc Document) {
	// Потрібно перевірити що документ містить поле `{cfg.PrimaryKey}` типу `string`
	if doc.Fields == nil {
		fmt.Println("[Collection Put] Error: Document is empty")
		return
	}
	pk := s.cfg.PrimaryKey
	fieldKey, exist := doc.Fields[pk]
	if !exist {
		fmt.Printf("[Collection Put] Error: Field '%s' is missing\n", pk)
		return
	}
	if fieldKey.Type != DocumentFieldTypeString {
		fmt.Printf("[Collection Put] Error: Field '%s' must be of type 'string'\n", pk)
		return
	}
	keyValue, ok := fieldKey.Value.(string)
	if !ok || strings.TrimSpace(keyValue) == "" {
		fmt.Printf("[Collection] Error: '%s' value is not a non-empty string\n", pk)
		return
	}
	if strings.TrimSpace(keyValue) == "" {
		fmt.Printf("[Collection] Error: '%s' value is empty\n", pk)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents[keyValue] = &doc
	fmt.Printf("[Collection] Document with %s='%s' added successfully\n", pk, keyValue)
}

func (s *Collection) Get(key string) (*Document, bool) {
	if strings.TrimSpace(key) == "" {
		fmt.Println("[Collection Get] Error: key is empty")
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	doc, ok := s.documents[key]
	return doc, ok
}

func (s *Collection) Delete(key string) bool {

	if strings.TrimSpace(key) == "" {
		fmt.Println("[Collection Delete] Error: key is empty")
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.documents[key]
	if ok {
		delete(s.documents, key)
		fmt.Printf("[Collection Delete] Document with key '%s' deleted successfully\n", key)
	}
	return ok
}

func (s *Collection) List() []Document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	docs := make([]Document, 0, len(s.documents))
	for _, doc := range s.documents {
		docs = append(docs, *doc)
	}
	return docs
}
