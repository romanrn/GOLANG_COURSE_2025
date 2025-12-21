package documentstore

import (
	"fmt"
	"strings"
	"sync" // 1. Імпортуємо пакет sync
)

type DocumentFieldType string

const (
	DocumentFieldTypeString DocumentFieldType = "string"
	DocumentFieldTypeNumber DocumentFieldType = "number"
	DocumentFieldTypeBool   DocumentFieldType = "bool"
	DocumentFieldTypeArray  DocumentFieldType = "array"
	DocumentFieldTypeObject DocumentFieldType = "object"
)

type DocumentField struct {
	Type  DocumentFieldType
	Value interface{}
}

type Document struct {
	Fields map[string]DocumentField
}

var documents = map[string]*Document{}

// RWMutex allows to multiply read or single write
var mu sync.RWMutex

func Put(doc *Document) {
	// 1. Перевірити що документ містить в мапі поле `key` типу `string`
	// 2. Додати Document до локальної мапи з документами

	if doc == nil || doc.Fields == nil {
		fmt.Println("[Store] Put Error: Document is nil or empty")
		return
	}

	// Check key field
	fieldKey, ok := doc.Fields["key"]
	if !ok {
		fmt.Println("[Store] Error: Field 'key' is missing")
		return
	}

	// Check key type
	if fieldKey.Type != DocumentFieldTypeString {
		fmt.Println("[Store] Error: Field 'key' must be of type 'string'")
		return
	}

	keyValue, ok := fieldKey.Value.(string)
	if !ok {
		fmt.Println("[Store] Error:  'key' value  is not a string")
		return
	}
	// Add document to store
	// WRITE LOCK- lock all
	mu.Lock()
	defer mu.Unlock() // unlock after exit

	documents[keyValue] = doc
	fmt.Printf("[Store] Document with key '%s' added successfully\n", keyValue)
}

func Get(key string) (*Document, bool) {
	// Потрібно повернути документ по ключу
	// Якщо документ знайдено, повертаємо `true` та поінтер на документ
	// Інакше повертаємо `false` та `nil`

	// Check for empty key
	if isKeyEmpty(key) {
		return nil, false
	}

	// READ LOCK  Read alloed; Put/Delete will wait
	mu.RLock()
	defer mu.RUnlock()

	// return document if exists
	doc, ok := documents[key]
	return doc, ok
}

func Delete(key string) bool {
	// Видаляємо документа по ключу.
	// Повертаємо `true` якщо ми знайшли і видалили документі
	// Повертаємо `false` якщо документ не знайдено

	// Check for empty key
	if isKeyEmpty(key) {
		return false
	}

	// WRITE LOCK - lock all
	mu.Lock()
	defer mu.Unlock()
	_, ok := documents[key]
	if ok {
		delete(documents, key)
		return true
	}
	return false
}

func List() []*Document {
	// Повертаємо список усіх документів

	// READ LOCK  Read alloed; Put/Delete will wait
	mu.RLock()
	defer mu.RUnlock()

	// Init list with capacity 9memory allocation
	list := make([]*Document, 0, len(documents))
	for _, doc := range documents {
		list = append(list, doc)
	}
	return list
}

func isKeyEmpty(key string) bool {
	if strings.TrimSpace(key) == "" {
		fmt.Println("[Store] Error:  'key' value is empty")
		return true
	}
	return false
}
