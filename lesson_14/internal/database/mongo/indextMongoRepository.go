package mongo

import (
	"context"
	"fmt"
	"lesson_14/internal/services"
	"lesson_14/internal/services/dto"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoIndexRepository struct {
	client *mongo.Client
	dbName string
	logger *slog.Logger
}

func NewMongoIndexRepository(client *mongo.Client, dbName string, logger *slog.Logger) services.IndexRepository {
	return &MongoIndexRepository{
		client: client,
		dbName: dbName,
		logger: logger.With("component", "NewMongoIndexRepository"),
	}
}

func (r *MongoIndexRepository) Create(ctx context.Context, collectionName string, fields []dto.IndexField, unique bool, indexName string) error {
	r.logger.Info("Mongo Repo Creating index in collection",
		slog.String("collectionName", collectionName),
		slog.Int("fieldsCount", len(fields)),
		slog.Bool("unique", unique))

	if len(fields) == 0 {
		return fmt.Errorf("at least one field is required for index")
	}

	db := r.client.Database(r.dbName)
	collection := db.Collection(collectionName)

	keys := bson.D{}
	for _, field := range fields {
		direction := field.Direction
		if direction != 1 && direction != -1 {
			direction = 1
		}
		keys = append(keys, bson.E{Key: field.Name, Value: direction})
	}

	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(unique),
	}
	if indexName != "" {
		indexModel.Options = indexModel.Options.SetName(indexName)
	}

	indexName, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		r.logger.Error("Failed to create index", "error", err)
		return fmt.Errorf("failed to create index: %w", err)
	}

	r.logger.Info("Index created successfully",
		slog.String("indexName", indexName),
		slog.String("collectionName", collectionName))
	return nil
}

func (r *MongoIndexRepository) Delete(ctx context.Context, collectionName, indexName string) error {

	r.logger.Info("Mongo Repo Deleting index from collection",
		slog.String("collectionName", collectionName),
		slog.String("indexName", indexName))

	if collectionName == "" {
		return fmt.Errorf("collection name is required")
	}

	if indexName == "" {
		return fmt.Errorf("index name is required")
	}

	db := r.client.Database(r.dbName)
	collection := db.Collection(collectionName)

	_, err := collection.Indexes().DropOne(ctx, indexName)
	if err != nil {
		r.logger.Error("Failed to delete index",
			slog.String("indexName", indexName),
			slog.String("error", err.Error()))
		return fmt.Errorf("failed to delete index: %w", err)
	}

	r.logger.Info("Index deleted successfully",
		slog.String("indexName", indexName),
		slog.String("collectionName", collectionName))
	return nil
}
