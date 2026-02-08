package documentstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// CollectionTestSuite defines a test suite for Collection
type CollectionTestSuite struct {
	suite.Suite
	collection *Collection
}

// SetupTest runs before each test
func (suite *CollectionTestSuite) SetupTest() {
	cfg := &CollectionConfig{
		PrimaryKey: "id",
	}
	suite.collection = NewCollection(cfg)
}

func TestCollectionTestSuite(t *testing.T) {
	suite.Run(t, new(CollectionTestSuite))
}

func TestNewCollection(t *testing.T) {
	t.Run("with nil config", func(t *testing.T) {
		col := NewCollection(nil)
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.cfg.PrimaryKey)
		assert.NotNil(t, col.documents)
		assert.Empty(t, col.documents)
	})

	t.Run("with custom config", func(t *testing.T) {
		cfg := &CollectionConfig{
			PrimaryKey: "userId",
		}
		col := NewCollection(cfg)
		assert.NotNil(t, col)
		assert.Equal(t, "userId", col.cfg.PrimaryKey)
		assert.NotNil(t, col.documents)
		assert.Empty(t, col.documents)
	})

	t.Run("with empty primary key", func(t *testing.T) {
		cfg := &CollectionConfig{
			PrimaryKey: "",
		}
		col := NewCollection(cfg)
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.cfg.PrimaryKey) // Should use default
	})

	t.Run("with whitespace primary key", func(t *testing.T) {
		cfg := &CollectionConfig{
			PrimaryKey: "   ",
		}
		col := NewCollection(cfg)
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.cfg.PrimaryKey) // Should use default
	})
}

func (suite *CollectionTestSuite) TestPut_Success() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user1",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "John Doe",
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.NoError(suite.T(), err)

	// Verify document was added
	retrieved, err := suite.collection.Get("user1")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), retrieved)
	assert.Equal(suite.T(), "user1", retrieved.Fields["id"].Value)
}

func (suite *CollectionTestSuite) TestPut_EmptyDocument() {
	doc := Document{
		Fields: nil,
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrEmptyDocument, err)
}

func (suite *CollectionTestSuite) TestPut_MissingPrimaryKey() {
	doc := Document{
		Fields: map[string]DocumentField{
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "John Doe",
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyMissing, err)
}

func (suite *CollectionTestSuite) TestPut_InvalidPrimaryKeyType() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeNumber,
				Value: 123,
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrValueTypeInvalid, err)
}

func (suite *CollectionTestSuite) TestPut_EmptyPrimaryKeyValue() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "",
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
}

func (suite *CollectionTestSuite) TestPut_WhitespacePrimaryKeyValue() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "   ",
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
}

func (suite *CollectionTestSuite) TestPut_NonStringValue() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: 12345,
			},
		},
	}

	err := suite.collection.Put(doc)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
}

func (suite *CollectionTestSuite) TestPut_UpdateExistingDocument() {
	doc1 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user123",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "John Doe",
			},
		},
	}

	doc2 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user123",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "Jane Doe",
			},
		},
	}

	err := suite.collection.Put(doc1)
	assert.NoError(suite.T(), err)

	err = suite.collection.Put(doc2)
	assert.NoError(suite.T(), err)

	// Should have updated the document
	retrieved, err := suite.collection.Get("user123")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "Jane Doe", retrieved.Fields["name"].Value)
}

// Test Get method
func (suite *CollectionTestSuite) TestGet_Success() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user456",
			},
			"email": {
				Type:  DocumentFieldTypeString,
				Value: "test@example.com",
			},
		},
	}

	_ = suite.collection.Put(doc)

	retrieved, err := suite.collection.Get("user456")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), retrieved)
	assert.Equal(suite.T(), "user456", retrieved.Fields["id"].Value)
	assert.Equal(suite.T(), "test@example.com", retrieved.Fields["email"].Value)
}

func (suite *CollectionTestSuite) TestGet_EmptyKey() {
	retrieved, err := suite.collection.Get("")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
	assert.Nil(suite.T(), retrieved)
}

func (suite *CollectionTestSuite) TestGet_WhitespaceKey() {
	retrieved, err := suite.collection.Get("   ")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
	assert.Nil(suite.T(), retrieved)
}

func (suite *CollectionTestSuite) TestGet_NotFound() {
	retrieved, err := suite.collection.Get("non-existent-key")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrDocumentNotFound, err)
	assert.Nil(suite.T(), retrieved)
}

func (suite *CollectionTestSuite) TestDelete_Success() {
	doc := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user789",
			},
		},
	}

	_ = suite.collection.Put(doc)

	err := suite.collection.Delete("user789")
	assert.NoError(suite.T(), err)

	retrieved, err := suite.collection.Get("user789")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrDocumentNotFound, err)
	assert.Nil(suite.T(), retrieved)
}

func (suite *CollectionTestSuite) TestDelete_EmptyKey() {
	err := suite.collection.Delete("")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
}

func (suite *CollectionTestSuite) TestDelete_WhitespaceKey() {
	err := suite.collection.Delete("   ")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrKeyEmpty, err)
}

func (suite *CollectionTestSuite) TestDelete_NotFound() {
	err := suite.collection.Delete("non-existent-key")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), ErrDocumentNotFound, err)
}

func (suite *CollectionTestSuite) TestList_Empty() {
	docs := suite.collection.List()
	assert.NotNil(suite.T(), docs)
	assert.Empty(suite.T(), docs)
}

func (suite *CollectionTestSuite) TestList_WithDocuments() {
	doc1 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user-1",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "User One",
			},
		},
	}

	doc2 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user-2",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "User Two",
			},
		},
	}

	doc3 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user-3",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "User Three",
			},
		},
	}

	_ = suite.collection.Put(doc1)
	_ = suite.collection.Put(doc2)
	_ = suite.collection.Put(doc3)

	docs := suite.collection.List()
	assert.NotNil(suite.T(), docs)
	assert.Len(suite.T(), docs, 3)

	foundIds := make(map[string]bool)
	for _, doc := range docs {
		id := doc.Fields["id"].Value.(string)
		foundIds[id] = true
	}

	assert.True(suite.T(), foundIds["user-1"])
	assert.True(suite.T(), foundIds["user-2"])
	assert.True(suite.T(), foundIds["user-3"])
}

func (suite *CollectionTestSuite) TestList_AfterDelete() {
	doc1 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user-1",
			},
		},
	}

	doc2 := Document{
		Fields: map[string]DocumentField{
			"id": {
				Type:  DocumentFieldTypeString,
				Value: "user-2",
			},
		},
	}

	_ = suite.collection.Put(doc1)
	_ = suite.collection.Put(doc2)

	docs := suite.collection.List()
	assert.Len(suite.T(), docs, 2)

	_ = suite.collection.Delete("user-1")

	docs = suite.collection.List()
	assert.Len(suite.T(), docs, 1)
	assert.Equal(suite.T(), "user-2", docs[0].Fields["id"].Value)
}

func TestCollection_CustomPrimaryKey(t *testing.T) {
	cfg := &CollectionConfig{PrimaryKey: "email"}
	col := NewCollection(cfg)

	doc := Document{
		Fields: map[string]DocumentField{
			"email": {
				Type:  DocumentFieldTypeString,
				Value: "user@example.com",
			},
			"name": {
				Type:  DocumentFieldTypeString,
				Value: "Test User",
			},
		},
	}

	err := col.Put(doc)
	assert.NoError(t, err)

	retrieved, err := col.Get("user@example.com")
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, "Test User", retrieved.Fields["name"].Value)
}

func TestCollection_EdgeCases(t *testing.T) {
	t.Run("document with multiple fields", func(t *testing.T) {
		cfg := &CollectionConfig{PrimaryKey: "id"}
		col := NewCollection(cfg)

		doc := Document{
			Fields: map[string]DocumentField{
				"id": {
					Type:  DocumentFieldTypeString,
					Value: "complex-doc",
				},
				"name": {
					Type:  DocumentFieldTypeString,
					Value: "Complex Document",
				},
				"age": {
					Type:  DocumentFieldTypeNumber,
					Value: 25,
				},
				"active": {
					Type:  DocumentFieldTypeBool,
					Value: true,
				},
				"tags": {
					Type:  DocumentFieldTypeArray,
					Value: []string{"tag1", "tag2"},
				},
			},
		}

		err := col.Put(doc)
		assert.NoError(t, err)

		retrieved, err := col.Get("complex-doc")
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, 5, len(retrieved.Fields))
	})

	t.Run("list returns copy not reference", func(t *testing.T) {
		cfg := &CollectionConfig{PrimaryKey: "id"}
		col := NewCollection(cfg)

		doc := Document{
			Fields: map[string]DocumentField{
				"id": {
					Type:  DocumentFieldTypeString,
					Value: "test-doc",
				},
			},
		}

		_ = col.Put(doc)

		list1 := col.List()
		list2 := col.List()

		// Lists should have same content but be different slices
		assert.Equal(t, len(list1), len(list2))
		assert.NotSame(t, &list1, &list2)
	})
}
