package api

type CreateDeleteCollectionRequest struct {
	Name string `json:"name" validate:"required"`
}

type CreateDeleteResponse struct {
	Ok         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	Message    string `json:"message,omitempty"`
	Collection string `json:"collection,omitempty"`
	Timestamp  int64  `json:"timestamp,omitempty"`
}

type ListCollectionResponse struct {
	Ok          bool          `json:"ok"`
	Error       string        `json:"error,omitempty"`
	Message     string        `json:"message,omitempty"`
	Collections []*Collection `json:"collections,omitempty"`
	Timestamp   int64         `json:"timestamp,omitempty"`
}

type Collection struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type CreateDocumentRequest struct {
	CollectionName string                 `json:"collection_name" validate:"required"`
	Document       map[string]interface{} `json:"document" validate:"required"`
}

type RetrieveListDocumentRequest struct {
	CollectionName string `json:"collection_name" validate:"required"`
}

type ListDocumentsResponse struct {
	Ok        bool        `json:"ok"`
	Error     string      `json:"error,omitempty"`
	Message   string      `json:"message,omitempty"`
	Documents []*Document `json:"documents" validate:"required"`
	Timestamp int64       `json:"timestamp,omitempty"`
}

type Document struct {
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data"`
}

type RetrieveDeleteDocumentRequest struct {
	CollectionName string `json:"collection_name" validate:"required"`
	Field          string `json:"field" validate:"required"`
	Value          string `json:"value" validate:"required"`
}

type GetDocumentResponse struct {
	Ok        bool      `json:"ok"`
	Document  *Document `json:"document,omitempty"`
	Error     string    `json:"error,omitempty"`
	Timestamp int64     `json:"timestamp"`
}

type CreateIndexRequest struct {
	CollectionName string       `json:"collection_name"`
	Fields         []IndexField `json:"fields"`
	Unique         bool         `json:"unique"`
	Name           string       `json:"name,omitempty"`
}

type IndexField struct {
	Name      string `json:"name"`
	Direction int    `json:"direction"`
}

type CreateIndexResponse struct {
	Message    string       `json:"message"`
	Collection string       `json:"collection"`
	Fields     []IndexField `json:"fields"`
	Unique     bool         `json:"unique"`
}

type DeleteIndexRequest struct {
	CollectionName string `json:"collection_name"`
	Name           string `json:"name"`
}
