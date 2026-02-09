package dto

type CreateDocumentDTO struct {
	CollectionName string
	Document       map[string]interface{}
}

type CreateIndexDTO struct {
	CollectionName string       `json:"collection_name"`
	Fields         []IndexField `json:"fields"`
	Unique         bool         `json:"unique"`
	Name           string       `json:"name,omitempty"`
}

type IndexField struct {
	Name      string `json:"name"`
	Direction int    `json:"direction"`
}
