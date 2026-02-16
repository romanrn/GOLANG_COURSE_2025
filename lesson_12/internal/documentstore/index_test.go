package documentstore

import (
	"testing"
)

func TestIndexBasicOperations(t *testing.T) {
	idx := NewIndex("name")

	// Test Insert
	idx.Insert("Alice", "doc1")
	idx.Insert("Bob", "doc2")
	idx.Insert("Charlie", "doc3")
	idx.Insert("Alice", "doc4") // Same value, different doc

	// Test GetAllDocKeys
	allKeys := idx.GetAllDocKeys()
	if len(allKeys) != 4 {
		t.Errorf("Expected 4 document keys, got %d", len(allKeys))
	}

	// Test Remove
	idx.Remove("Alice", "doc1")
	allKeys = idx.GetAllDocKeys()
	if len(allKeys) != 3 {
		t.Errorf("After removal, expected 3 document keys, got %d", len(allKeys))
	}

	// Remove last occurrence of Alice
	idx.Remove("Alice", "doc4")
	allKeys = idx.GetAllDocKeys()
	if len(allKeys) != 2 {
		t.Errorf("After removing all Alice, expected 2 document keys, got %d", len(allKeys))
	}
}

func TestIndexRangeQuery(t *testing.T) {
	idx := NewIndex("age")

	idx.Insert("20", "doc1")
	idx.Insert("25", "doc2")
	idx.Insert("30", "doc3")
	idx.Insert("35", "doc4")
	idx.Insert("40", "doc5")

	// Test range query without bounds
	results := idx.RangeQuery(nil, nil, false)
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Test range query with min value
	minVal := "25"
	results = idx.RangeQuery(&minVal, nil, false)
	if len(results) != 4 {
		t.Errorf("Expected 4 results with min=25, got %d", len(results))
	}

	// Test range query with max value
	maxVal := "30"
	results = idx.RangeQuery(nil, &maxVal, false)
	if len(results) != 3 {
		t.Errorf("Expected 3 results with max=30, got %d", len(results))
	}

	// Test range query with both bounds
	results = idx.RangeQuery(&minVal, &maxVal, false)
	if len(results) != 2 {
		t.Errorf("Expected 2 results with min=25 and max=30, got %d", len(results))
	}

	// Test descending order
	results = idx.RangeQuery(nil, nil, true)
	if len(results) != 5 {
		t.Errorf("Expected 5 results in descending order, got %d", len(results))
	}
}

func TestCollectionCreateIndex(t *testing.T) {
	cfg := &CollectionConfig{PrimaryKey: "id"}
	coll := NewCollection(cfg)

	// Add some documents
	doc1 := Document{
		Fields: map[string]DocumentField{
			"id":   {Type: DocumentFieldTypeString, Value: "1"},
			"name": {Type: DocumentFieldTypeString, Value: "Alice"},
			"age":  {Type: DocumentFieldTypeString, Value: "25"},
		},
	}
	doc2 := Document{
		Fields: map[string]DocumentField{
			"id":   {Type: DocumentFieldTypeString, Value: "2"},
			"name": {Type: DocumentFieldTypeString, Value: "Bob"},
			"age":  {Type: DocumentFieldTypeString, Value: "30"},
		},
	}

	if err := coll.Put(doc1); err != nil {
		t.Fatalf("Failed to put doc1: %v", err)
	}
	if err := coll.Put(doc2); err != nil {
		t.Fatalf("Failed to put doc2: %v", err)
	}

	// Create index on name field
	if err := coll.CreateIndex("name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Try to create the same index again (should fail)
	if err := coll.CreateIndex("name"); err != ErrIndexAlreadyExists {
		t.Errorf("Expected ErrIndexAlreadyExists, got: %v", err)
	}

	// Verify index was created
	coll.mu.RLock()
	if _, exists := coll.indexes["name"]; !exists {
		t.Error("Index 'name' was not created")
	}
	coll.mu.RUnlock()
}

func TestCollectionDeleteIndex(t *testing.T) {
	cfg := &CollectionConfig{PrimaryKey: "id"}
	coll := NewCollection(cfg)

	// Create an index
	if err := coll.CreateIndex("name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Delete the index
	if err := coll.DeleteIndex("name"); err != nil {
		t.Fatalf("Failed to delete index: %v", err)
	}

	// Try to delete non-existent index (should fail)
	if err := coll.DeleteIndex("name"); err != ErrIndexNotFound {
		t.Errorf("Expected ErrIndexNotFound, got: %v", err)
	}

	// Verify index was deleted
	coll.mu.RLock()
	if _, exists := coll.indexes["name"]; exists {
		t.Error("Index 'name' still exists after deletion")
	}
	coll.mu.RUnlock()
}

func TestCollectionQuery(t *testing.T) {
	cfg := &CollectionConfig{PrimaryKey: "id"}
	coll := NewCollection(cfg)

	// Add documents
	docs := []Document{
		{
			Fields: map[string]DocumentField{
				"id":   {Type: DocumentFieldTypeString, Value: "1"},
				"name": {Type: DocumentFieldTypeString, Value: "Alice"},
				"age":  {Type: DocumentFieldTypeString, Value: "25"},
			},
		},
		{
			Fields: map[string]DocumentField{
				"id":   {Type: DocumentFieldTypeString, Value: "2"},
				"name": {Type: DocumentFieldTypeString, Value: "Bob"},
				"age":  {Type: DocumentFieldTypeString, Value: "30"},
			},
		},
		{
			Fields: map[string]DocumentField{
				"id":   {Type: DocumentFieldTypeString, Value: "3"},
				"name": {Type: DocumentFieldTypeString, Value: "Charlie"},
				"age":  {Type: DocumentFieldTypeString, Value: "35"},
			},
		},
	}

	for _, doc := range docs {
		if err := coll.Put(doc); err != nil {
			t.Fatalf("Failed to put document: %v", err)
		}
	}

	// Create index on age field
	if err := coll.CreateIndex("age"); err != nil {
		t.Fatalf("Failed to create index on age: %v", err)
	}

	// Query without index (should fail)
	_, err := coll.Query("name", QueryParams{})
	if err != ErrIndexNotFound {
		t.Errorf("Expected ErrIndexNotFound for non-indexed field, got: %v", err)
	}

	// Query all documents
	results, err := coll.Query("age", QueryParams{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Query with min value
	minAge := "30"
	results, err = coll.Query("age", QueryParams{MinValue: &minAge})
	if err != nil {
		t.Fatalf("Query with MinValue failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results with age >= 30, got %d", len(results))
	}

	// Query with max value
	maxAge := "30"
	results, err = coll.Query("age", QueryParams{MaxValue: &maxAge})
	if err != nil {
		t.Fatalf("Query with MaxValue failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results with age <= 30, got %d", len(results))
	}

	// Query with range
	minAge = "26"
	maxAge = "34"
	results, err = coll.Query("age", QueryParams{MinValue: &minAge, MaxValue: &maxAge})
	if err != nil {
		t.Fatalf("Query with range failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result with age between 26 and 34, got %d", len(results))
	}
	if len(results) == 1 && results[0].Fields["name"].Value != "Bob" {
		t.Errorf("Expected Bob, got %v", results[0].Fields["name"].Value)
	}
}

func TestIndexUpdateOnDocumentOperations(t *testing.T) {
	cfg := &CollectionConfig{PrimaryKey: "id"}
	coll := NewCollection(cfg)

	// Create index first
	if err := coll.CreateIndex("status"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Add document
	doc1 := Document{
		Fields: map[string]DocumentField{
			"id":     {Type: DocumentFieldTypeString, Value: "1"},
			"status": {Type: DocumentFieldTypeString, Value: "active"},
		},
	}
	if err := coll.Put(doc1); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}

	// Query should return 1 result
	results, _ := coll.Query("status", QueryParams{})
	if len(results) != 1 {
		t.Errorf("Expected 1 result after insert, got %d", len(results))
	}

	// Update document (change status)
	doc1Updated := Document{
		Fields: map[string]DocumentField{
			"id":     {Type: DocumentFieldTypeString, Value: "1"},
			"status": {Type: DocumentFieldTypeString, Value: "inactive"},
		},
	}
	if err := coll.Put(doc1Updated); err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// Query for old status should return 0 results
	oldStatus := "active"
	results, _ = coll.Query("status", QueryParams{MinValue: &oldStatus, MaxValue: &oldStatus})
	if len(results) != 0 {
		t.Errorf("Expected 0 results for old status, got %d", len(results))
	}

	// Query for new status should return 1 result
	newStatus := "inactive"
	results, _ = coll.Query("status", QueryParams{MinValue: &newStatus, MaxValue: &newStatus})
	if len(results) != 1 {
		t.Errorf("Expected 1 result for new status, got %d", len(results))
	}

	// Delete document
	if err := coll.Delete("1"); err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Query should return 0 results after delete
	results, _ = coll.Query("status", QueryParams{})
	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete, got %d", len(results))
	}
}

func TestStoreDumpWithIndexes(t *testing.T) {
	store := NewStore()
	cfg := &CollectionConfig{PrimaryKey: "id"}

	// Create collection
	coll, err := store.CreateCollection("users", cfg)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Add documents
	doc := Document{
		Fields: map[string]DocumentField{
			"id":   {Type: DocumentFieldTypeString, Value: "1"},
			"name": {Type: DocumentFieldTypeString, Value: "Alice"},
		},
	}
	if err := coll.Put(doc); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}

	// Create index
	if err := coll.CreateIndex("name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Dump store
	dumpData, err := store.Dump()
	if err != nil {
		t.Fatalf("Failed to dump store: %v", err)
	}

	// Restore from dump
	restoredStore, err := NewStoreFromDump(dumpData)
	if err != nil {
		t.Fatalf("Failed to restore from dump: %v", err)
	}

	// Verify collection and index exist
	restoredColl, err := restoredStore.GetCollection("users")
	if err != nil {
		t.Fatalf("Failed to get collection from restored store: %v", err)
	}

	// Check if index exists
	restoredColl.mu.RLock()
	if _, exists := restoredColl.indexes["name"]; !exists {
		t.Error("Index 'name' was not restored from dump")
	}
	restoredColl.mu.RUnlock()

	// Verify query works on restored index
	results, err := restoredColl.Query("name", QueryParams{})
	if err != nil {
		t.Fatalf("Query failed on restored store: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result from restored store, got %d", len(results))
	}
}
