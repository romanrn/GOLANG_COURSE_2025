package mongo

import (
	"context"
	"fmt"
	"lesson_14/internal/services"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDocumentRepository struct {
	client *mongo.Client
	dbName string
	logger *slog.Logger
}

func NewMongoDocumentRepository(client *mongo.Client, dbName string, logger *slog.Logger) services.DocumentRepository {
	return &MongoDocumentRepository{
		client: client,
		dbName: dbName,
		logger: logger.With("component", "NewMongoDocumentRepository"),
	}
}

func (r *MongoDocumentRepository) Create(ctx context.Context, collectionName string, document map[string]interface{}) error {
	r.logger.Info("Mongo Repo Creating document in collection", "collectionName", collectionName)

	db := r.client.Database(r.dbName)
	collection := db.Collection(collectionName)
	_, err := collection.InsertOne(ctx, document)

	if err != nil {
		r.logger.Error("Failed to insert document", "error", err)
		return err
	}

	r.logger.Info("Document created successfully", "collectionName", collectionName)
	return nil
}

func (r *MongoDocumentRepository) List(ctx context.Context, collectionName string) ([]*services.Document, error) {

	collection := r.client.Database(r.dbName).Collection(collectionName)
	filter := bson.M{}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var documents []*services.Document
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, fmt.Errorf("failed to decode documents: %w", err)
	}

	return documents, nil
}

func (r *MongoDocumentRepository) Delete(ctx context.Context, collectionName, field, value string) error {
	r.logger.Info("Mongo Repo Deleting document by field",
		"collectionName", collectionName,
		"field", field,
		"value", value)

	collection := r.client.Database(r.dbName).Collection(collectionName)
	filter := bson.M{field: value}

	result, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		r.logger.Error("Failed to delete document", "error", err)
		return fmt.Errorf("failed to delete document: %w", err)
	}

	if result.DeletedCount == 0 {
		r.logger.Warn("Document not found for deletion", "field", field, "value", value)
		return fmt.Errorf("document not found with %s=%s", field, value)
	}

	r.logger.Info("Document deleted successfully", "deletedCount", result.DeletedCount)
	return nil
}

func (r *MongoDocumentRepository) Get(ctx context.Context, collectionName, field, value string) (*services.Document, error) {
	r.logger.Info("Mongo Repo Getting document by field",
		"collectionName", collectionName,
		"field", field,
		"value", value)

	collection := r.client.Database(r.dbName).Collection(collectionName)
	filter := bson.M{field: value}

	var document services.Document
	err := collection.FindOne(ctx, filter).Decode(&document)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			r.logger.Warn("Document not found", "field", field, "value", value)
			return nil, fmt.Errorf("document not found with %s=%s", field, value)
		}
		r.logger.Error("Failed to find document", "error", err)
		return nil, fmt.Errorf("failed to find document: %w", err)
	}

	r.logger.Info("Document found successfully", "id", document.ID.Hex())
	return &document, nil
}
