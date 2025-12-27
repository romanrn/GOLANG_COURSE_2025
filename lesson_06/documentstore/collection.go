package documentstore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrKeyEmpty         = errors.New("[Collection] Error: key is empty")
	ErrDocumentNotFound = errors.New("document not found")
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
	pkgLogger.Info("New collection is created")
	return &Collection{
		cfg:       defaultCfg,
		documents: make(map[string]*Document),
	}
}

func (s *Collection) Put(doc Document) {
	// Потрібно перевірити що документ містить поле `{cfg.PrimaryKey}` типу `string`
	if doc.Fields == nil {
		pkgLogger.Error("[Collection Put] Error: Document is empty")
		return
	}
	pk := s.cfg.PrimaryKey
	fieldKey, exist := doc.Fields[pk]
	if !exist {
		pkgLogger.Error("primary key field is missing", "field", pk)
		return
	}
	if fieldKey.Type != DocumentFieldTypeString {
		pkgLogger.Error("[Collection Put] Error: Field  must be of type 'string", "field", pk)
		return
	}
	keyValue, ok := fieldKey.Value.(string)
	if !ok || strings.TrimSpace(keyValue) == "" {
		pkgLogger.Error("[Collection] Error: value is not a non-empty string", "value", pk)
		return
	}
	if strings.TrimSpace(keyValue) == "" {
		pkgLogger.Error("[Collection] Error: value is empty", "value", pk)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents[keyValue] = &doc
	pkgLogger.Info(fmt.Sprintf("[Collection] Document with %s='%s' added", pk, keyValue))
}

func (s *Collection) Get(key string) (*Document, error) {
	if strings.TrimSpace(key) == "" {

		pkgLogger.Error("[Collection Get] Error: key is empty")
		return nil, ErrKeyEmpty
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	doc, ok := s.documents[key]
	if !ok {
		fmt.Printf("[Collection Get] Document with key '%s' not found\n", key)
		pkgLogger.Error(fmt.Sprintf("[Collection Get] Document with key %s  not found", key))
		return nil, ErrDocumentNotFound
	}
	return doc, nil
}

func (s *Collection) Delete(key string) error {

	if strings.TrimSpace(key) == "" {
		pkgLogger.Error("[Collection Delete] Error: key is empty")
		return ErrKeyEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.documents[key]
	if ok {
		delete(s.documents, key)
		pkgLogger.Error(fmt.Sprintf("[Collection Delete] Document with key '%s' deleted successfully", key))
	} else {
		pkgLogger.Error(fmt.Sprintf("[Collection Delete] Document with key '%s' not found", key))
		return ErrDocumentNotFound
	}

	return nil
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
