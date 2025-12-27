package documentstore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrKeyEmpty         = errors.New("[Collection] Error: key is empty")
	ErrKeyMissing       = errors.New("[Collection] Error: key is mossing")
	ErrValueEmpty       = errors.New("[Collection] Error: value is empty")
	ErrDocumentNotFound = errors.New("document not found")
	ErrEmptyDocument    = errors.New("Collection] Error: Provided document is empty")
	ErrValueTypeInvalid = errors.New("[Collection] Error: Field  must be of type string")
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

func (s *Collection) Put(doc Document) error {
	// Потрібно перевірити що документ містить поле `{cfg.PrimaryKey}` типу `string`
	if doc.Fields == nil {
		pkgLogger.Error("[Collection Put] Error: Document is empty")
		return ErrEmptyDocument
	}
	pk := s.cfg.PrimaryKey
	fieldKey, exist := doc.Fields[pk]
	if !exist {
		pkgLogger.Error("primary key field is missing", "field", pk)
		return ErrKeyMissing
	}
	if fieldKey.Type != DocumentFieldTypeString {
		pkgLogger.Error("[Collection Put] Error: Field  must be of type 'string", "field", pk)
		return ErrValueTypeInvalid
	}
	keyValue, ok := fieldKey.Value.(string)
	if !ok || strings.TrimSpace(keyValue) == "" {
		pkgLogger.Error("[Collection] Error: value is not a non-empty string", "value", pk)
		return ErrKeyEmpty
	}
	if strings.TrimSpace(keyValue) == "" {
		pkgLogger.Error("[Collection] Error: value is empty", "value", pk)
		return ErrValueEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents[keyValue] = &doc
	pkgLogger.Info(fmt.Sprintf("[Collection] Document with %s='%s' added", pk, keyValue))

	return nil
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

	if !ok {
		pkgLogger.Error(fmt.Sprintf("[Collection Delete] Document with key '%s' not found", key))
		return ErrDocumentNotFound
	}

	delete(s.documents, key)
	pkgLogger.Error(fmt.Sprintf("[Collection Delete] Document with key '%s' deleted successfully", key))

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
