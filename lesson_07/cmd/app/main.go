package main

import (
	"errors"
	"fmt"
	"lesson_07/internal/documentstore"
	"log"
	"log/slog"
	"os"
)

func main() {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main"))
	//documentstore.SetLogger(logger)
	logger.Info("=== DocumentStore ===")

	// Create a new document store
	store := documentstore.NewStore()
	//fmt.Println("\n=== Create Users ===")
	usersCollection, err := store.CreateCollection("users", &documentstore.CollectionConfig{
		PrimaryKey: "key",
	})
	switch {
	case err == nil:
		logger.Info("=== Created collection 'users' ===")
	case errors.Is(err, documentstore.ErrCollectionAlreadyExists):
		log.Fatal(err)
	case errors.Is(err, documentstore.ErrCollectionInvalidNameOrKey):
		log.Fatal(err)
	default:
		log.Fatal(err)
	}

	doc1 := documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"key": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "user1",
			},
			"name": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "John Doe",
			},
			"age": {
				Type:  documentstore.DocumentFieldTypeNumber,
				Value: 30,
			},
		},
	}

	doc2 := documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"key": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "user2",
			},
			"name": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "Jane Smith",
			},
			"age": {
				Type:  documentstore.DocumentFieldTypeNumber,
				Value: 25,
			},
		},
	}

	if usersCollection.Put(doc1) != nil {
		logger.Error("failed to put a doc", slog.Any("error", err))
	}
	if usersCollection.Put(doc2) != nil {
		logger.Error("failed to put a doc", slog.Any("error", err))
	}

	if store.DumpToFile("dump1.json") != nil {
		logger.Error("failed to dump to file", slog.Any("error", err))
	}
	store2, err := documentstore.NewStoreFromFile(("dump1.json"))
	if err != nil {
		log.Fatal(err)
	}
	collection2, err := store2.GetCollection("users")
	listDocs := collection2.List()

	logger.Info(fmt.Sprintf("=== Total documents: === '%d' ", len(listDocs)))
	for _, doc := range listDocs {
		logger.Info(fmt.Sprintf("%s: %s", doc.Fields["key"].Value, doc.Fields["name"].Value))
	}
}
