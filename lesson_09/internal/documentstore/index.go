package documentstore

import (
	"sort"
	"sync"
)

// Index is a data structure that maintains sorted field values and maps them to document keys
// It provides O(log n) search complexity for queries
type Index struct {
	fieldName string
	// sortedKeys contains unique field values in sorted order
	sortedKeys []string
	// valueToDocKeys maps field values to sets of document primary keys
	valueToDocKeys map[string]map[string]bool
	mu             sync.RWMutex
}

// NewIndex creates a new index for a given field
func NewIndex(fieldName string) *Index {
	return &Index{
		fieldName:      fieldName,
		sortedKeys:     make([]string, 0),
		valueToDocKeys: make(map[string]map[string]bool),
	}
}

// Finalize sorts the keys after bulk insert is complete
func (idx *Index) Finalize() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	sort.Strings(idx.sortedKeys)
}

func (idx *Index) Insert(value string, docKey string) {
	idx.insert(value, docKey, true)
}

// InsertUnsorted adds a document key to the index under the given field value
// used during bulk inserts
func (idx *Index) InsertUnsorted(value string, docKey string) {
	idx.insert(value, docKey, false)
}

// Insert adds a document key to the index under the given field value
// shouldSort=false used during bulk inserts
func (idx *Index) insert(value string, docKey string, shouldSort bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if this value already exists in the index
	if _, exists := idx.valueToDocKeys[value]; !exists {
		// Add new value to the map
		idx.valueToDocKeys[value] = make(map[string]bool)
		// Insert value into sorted slice and maintain sort order
		idx.sortedKeys = append(idx.sortedKeys, value)
		// Sort only if requested
		if shouldSort {
			sort.Strings(idx.sortedKeys)
		}
	}

	// Add document key to the set for this value
	idx.valueToDocKeys[value][docKey] = true
}

// Remove removes a document key from the index for the given field value
func (idx *Index) Remove(value string, docKey string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if value exists
	docKeys, exists := idx.valueToDocKeys[value]
	if !exists {
		return
	}

	// Remove document key from the set
	delete(docKeys, docKey)

	// If no more documents have this value, remove the value entirely
	if len(docKeys) == 0 {
		delete(idx.valueToDocKeys, value)
		// Remove from sorted keys
		for i, k := range idx.sortedKeys {
			if k == value {
				idx.sortedKeys = append(idx.sortedKeys[:i], idx.sortedKeys[i+1:]...)
				break
			}
		}
	}
}

// RangeQuery performs a range query on the index
// Returns document keys where field value is between minValue and maxValue (inclusive)
// If desc is true, results are returned in descending order
func (idx *Index) RangeQuery(minValue *string, maxValue *string, desc bool) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Determine which keys fall within the range
	// Process keys in the appropriate order based on desc flag
	keysToProcess := idx.getKeysToProcess(minValue, maxValue, desc)

	// Collect all document keys for the selected field values
	return idx.collectDocKeys(keysToProcess)
}

func (idx *Index) getKeysToProcess(minValue *string, maxValue *string, desc bool) []string {
	var result []string
	if desc {
		// Iterate backwards for descending order
		for i := len(idx.sortedKeys) - 1; i >= 0; i-- {
			if idx.isKeyInRange(idx.sortedKeys[i], minValue, maxValue) {
				result = append(result, idx.sortedKeys[i])
			}
		}
	} else {
		// Iterate forwards for ascending order
		for _, key := range idx.sortedKeys {
			if idx.isKeyInRange(key, minValue, maxValue) {
				result = append(result, key)
			}
		}
	}
	return result
}

// isKeyInRange checks if a key falls within the min/max range
func (idx *Index) isKeyInRange(key string, minValue *string, maxValue *string) bool {
	if minValue != nil && key < *minValue {
		return false
	}
	if maxValue != nil && key > *maxValue {
		return false
	}
	return true
}

func (idx *Index) collectDocKeys(keys []string) []string {
	var result []string

	for _, key := range keys {
		if docKeys, exists := idx.valueToDocKeys[key]; exists {
			for docKey := range docKeys {
				result = append(result, docKey)
			}
		}
	}

	return result
}

// GetAllDocKeys returns all document keys in the index
func (idx *Index) GetAllDocKeys() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]string, 0)

	for _, docKeys := range idx.valueToDocKeys {
		for docKey := range docKeys {
			result = append(result, docKey)
		}
	}

	return result
}
