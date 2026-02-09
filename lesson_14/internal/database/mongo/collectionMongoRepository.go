package mongo

import (
	"context"
	"fmt"
	"lesson_14/internal/services"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoCollectionRepository struct {
	client *mongo.Client
	dbName string
	logger *slog.Logger
}

func NewMongoCollectionRepository(client *mongo.Client, dbName string, logger *slog.Logger) services.CollectionRepository {
	return &MongoCollectionRepository{
		client: client,
		dbName: dbName,
		logger: logger.With("component", "NewMongoCollectionRepository"),
	}
}

func (r *MongoCollectionRepository) Create(ctx context.Context, name string) error {
	r.logger.Info("Mongo Repo Creating collection", "name", name)

	db := r.client.Database(r.dbName)

	names, err := db.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		r.logger.Error("Failed to check collection existence", "name", name, "error", err)
		return fmt.Errorf("failed to check collection existence: %w", err)
	}

	if len(names) > 0 {
		r.logger.Warn("Collection already exists", "name", name)
		return fmt.Errorf("collection %s already exists", name)
	}

	err = db.CreateCollection(ctx, name)
	if err != nil {
		r.logger.Error("Failed to create collection", "name", name, "error", err)
		return fmt.Errorf("failed to create collection %s: %w", name, err)
	}

	r.logger.Info("Collection created successfully", "name", name)
	return nil
}

func (r *MongoCollectionRepository) List(ctx context.Context) ([]*services.Collection, error) {
	db := r.client.Database(r.dbName)

	names, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		r.logger.Error("Failed to list collections", "error", err)
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	collections := make([]*services.Collection, 0, len(names))
	for _, name := range names {
		collections = append(collections, &services.Collection{
			Name: name,
		})
	}

	r.logger.Info("Listed collections", "count", len(collections))
	return collections, nil
}

func (r *MongoCollectionRepository) Delete(ctx context.Context, name string) error {
	r.logger.Info("Mongo Repo Deleting collection", "name", name)

	db := r.client.Database(r.dbName)

	names, err := db.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		r.logger.Error("Failed to check collection existence", "name", name, "error", err)
		return fmt.Errorf("failed to check collection existence: %w", err)
	}

	if len(names) == 0 {
		r.logger.Warn("Collection does not exist", "name", name)
		return fmt.Errorf("collection %s does not exist", name)
	}

	err = db.Collection(name).Drop(ctx)
	if err != nil {
		r.logger.Error("Failed to delete collection", "name", name, "error", err)
		return fmt.Errorf("failed to delete collection %s: %w", name, err)
	}

	r.logger.Info("Collection deleted successfully", "name", name)

	return nil
}
