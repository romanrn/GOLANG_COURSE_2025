package documentstore

// DocumentInterface formalizes the public contract for a document.
type DocumentInterface interface {
	GetFields() map[string]DocumentField
}

// CollectionInterface describes the public API for a collection.
type CollectionInterface interface {
	Put(doc Document)
	Get(key string) (*Document, bool)
	Delete(key string) bool
	List() []Document
}

// StoreInterface describes the API for a document store.
type StoreInterface interface {
	CreateCollection(name string, cfg *CollectionConfig) (bool, *Collection)
	GetCollection(name string) (*Collection, bool)
	DeleteCollection(name string) bool
}
