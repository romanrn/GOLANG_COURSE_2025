package documentstore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrKeyEmpty           = errors.New("[Collection] Error: key is empty")
	ErrKeyMissing         = errors.New("[Collection] Error: key is mossing")
	ErrValueEmpty         = errors.New("[Collection] Error: value is empty")
	ErrDocumentNotFound   = errors.New("document not found")
	ErrEmptyDocument      = errors.New("Collection] Error: Provided document is empty")
	ErrValueTypeInvalid   = errors.New("[Collection] Error: Field  must be of type string")
	ErrFieldNameEmpty     = errors.New("[Collection] Error: field name  is empty")
	ErrIndexAlreadyExists = errors.New("[Collection] Error: index already exists")
	ErrIndexNotFound      = errors.New("[Collection] Error: index not found")
)

type Collection struct {
	cfg       CollectionConfig
	documents map[string]*Document
	indexes   map[string]*Index // fieldName -> Index
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
		indexes:   make(map[string]*Index),
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

	// Store old document if it exists (for index updates)
	oldDoc, docExists := s.documents[keyValue]

	s.documents[keyValue] = &doc
	pkgLogger.Info(fmt.Sprintf("[Collection] Document with %s='%s' added", pk, keyValue))

	// Update all indexes
	for fieldName, idx := range s.indexes {
		// Remove old value from index if document existed
		if docExists && oldDoc != nil {
			if oldField, exists := oldDoc.Fields[fieldName]; exists && oldField.Type == DocumentFieldTypeString {
				if oldValue, ok := oldField.Value.(string); ok {
					idx.Remove(oldValue, keyValue)
				}
			}
		}

		// Add new value to index
		if field, exists := doc.Fields[fieldName]; exists && field.Type == DocumentFieldTypeString {
			if value, ok := field.Value.(string); ok {
				idx.Insert(value, keyValue)
			}
		}
	}

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
	doc, ok := s.documents[key]

	if !ok {
		pkgLogger.Error(fmt.Sprintf("[Collection Delete] Document with key '%s' not found", key))
		return ErrDocumentNotFound
	}

	// Update all indexes - remove document from indexes
	for fieldName, idx := range s.indexes {
		if field, exists := doc.Fields[fieldName]; exists && field.Type == DocumentFieldTypeString {
			if value, ok := field.Value.(string); ok {
				idx.Remove(value, key)
			}
		}
	}

	delete(s.documents, key)
	pkgLogger.Info(fmt.Sprintf("[Collection Delete] Document with key '%s' deleted successfully", key))

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

// QueryParams defines parameters for querying indexed fields
type QueryParams struct {
	Desc     bool    // Визначає в якому порядку повертати дані
	MinValue *string // Визначає мінімальне значення поля для фільтрації
	MaxValue *string // Визначає максимальне значення поля для фільтрації
}

// CreateIndex creates an index for the specified field in the collection
func (c *Collection) CreateIndex(fieldName string) error {
	if strings.TrimSpace(fieldName) == "" {
		pkgLogger.Error("[Collection CreateIndex] Error: fieldName is empty")
		return ErrFieldNameEmpty
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if index already exists
	if _, exists := c.indexes[fieldName]; exists {
		pkgLogger.Error("[Collection CreateIndex] Error: index already exists", "field", fieldName)
		return ErrIndexAlreadyExists
	}

	// Create new index
	idx := NewIndex(fieldName)

	// Populate index with existing documents
	for docKey, doc := range c.documents {
		if doc == nil {
			continue
		}
		// Only index string fields
		if field, exists := doc.Fields[fieldName]; exists && field.Type == DocumentFieldTypeString {
			if value, ok := field.Value.(string); ok {
				idx.InsertUnsorted(value, docKey)
			}
		}
	}

	// Sort once at the end
	idx.Finalize()

	c.indexes[fieldName] = idx
	pkgLogger.Info("[Collection CreateIndex] Index created successfully", "field", fieldName)

	return nil
}

// DeleteIndex removes an index for the specified field
func (с *Collection) DeleteIndex(fieldName string) error {
	if strings.TrimSpace(fieldName) == "" {
		pkgLogger.Error("[Collection DeleteIndex] Error: fieldName is empty")
		return ErrFieldNameEmpty
	}

	с.mu.Lock()
	defer с.mu.Unlock()

	// Check if index exists
	if _, exists := с.indexes[fieldName]; !exists {
		pkgLogger.Error("[Collection DeleteIndex] Error: index not found", "field", fieldName)
		return ErrIndexNotFound
	}

	delete(с.indexes, fieldName)
	pkgLogger.Info("[Collection DeleteIndex] Index deleted successfully", "field", fieldName)

	return nil
}

// Query performs a query on an indexed field and returns matching documents
func (s *Collection) Query(fieldName string, params QueryParams) ([]Document, error) {
	if strings.TrimSpace(fieldName) == "" {
		pkgLogger.Error("[Collection Query] Error: fieldName is empty")
		return nil, ErrFieldNameEmpty
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if index exists
	idx, exists := s.indexes[fieldName]
	if !exists {
		pkgLogger.Error("[Collection Query] Error: index not found", "field", fieldName)
		return nil, ErrIndexNotFound
	}

	// Perform range query on the index
	docKeys := idx.RangeQuery(params.MinValue, params.MaxValue, params.Desc)

	// Retrieve documents for the matching keys
	result := make([]Document, 0, len(docKeys))
	for _, docKey := range docKeys {
		if doc, exists := s.documents[docKey]; exists && doc != nil {
			result = append(result, *doc)
		}
	}

	pkgLogger.Info("[Collection Query] Query executed successfully",
		"field", fieldName,
		"results", len(result))

	return result, nil
}
