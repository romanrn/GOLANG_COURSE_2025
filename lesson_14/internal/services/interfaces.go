package services

import (
	"context"

	"lesson_14/internal/services/dto"
)

type CollectionService interface {
	CreateCollection(ctx context.Context, name string) error
	ListCollection(ctx context.Context) ([]*Collection, error)
	DeleteCollection(ctx context.Context, name string) error
}

type CollectionRepository interface {
	Create(ctx context.Context, name string) error
	List(ctx context.Context) ([]*Collection, error)
	Delete(ctx context.Context, name string) error
}

type DocumentService interface {
	CreateDocument(ctx context.Context, doc *dto.CreateDocumentDTO) error
	ListDocuments(ctx context.Context, collectionName string) ([]*Document, error)
	DeleteDocument(ctx context.Context, collectionName, field, value string) error
	GetDocument(ctx context.Context, collectionName, field, value string) (*Document, error)
}

type DocumentRepository interface {
	Create(ctx context.Context, collectionName string, document map[string]interface{}) error
	List(ctx context.Context, collectionName string) ([]*Document, error)
	Delete(ctx context.Context, collectionName, field, value string) error
	Get(ctx context.Context, collectionName, field, value string) (*Document, error)
}

type IndexService interface {
	CreateIndex(ctx context.Context, doc *dto.CreateIndexDTO) error
	DeleteIndex(ctx context.Context, collectionName, indexName string) error
}
type IndexRepository interface {
	Create(ctx context.Context, collectionName string, fields []dto.IndexField, unique bool, indexName string) error
	Delete(ctx context.Context, collectionName, indexName string) error
}
