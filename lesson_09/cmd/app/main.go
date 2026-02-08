package main

import (
	"fmt"
	"lesson_09/internal/documentstore"
	"log"
	"log/slog"
	"os"
)

func main() {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main"))
	//documentstore.SetLogger(logger)
	logger.Info("=== DocumentStore ===")

	// Create a new store
	store := documentstore.NewStore()

	// Create a collection
	cfg := &documentstore.CollectionConfig{PrimaryKey: "id"}
	users, err := store.CreateCollection("users", cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Add some documents
	users.Put(documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"id":     {Type: documentstore.DocumentFieldTypeString, Value: "1"},
			"name":   {Type: documentstore.DocumentFieldTypeString, Value: "Alice"},
			"age":    {Type: documentstore.DocumentFieldTypeString, Value: "25"},
			"status": {Type: documentstore.DocumentFieldTypeString, Value: "active"},
		},
	})

	users.Put(documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"id":     {Type: documentstore.DocumentFieldTypeString, Value: "2"},
			"name":   {Type: documentstore.DocumentFieldTypeString, Value: "Bob"},
			"age":    {Type: documentstore.DocumentFieldTypeString, Value: "30"},
			"status": {Type: documentstore.DocumentFieldTypeString, Value: "inactive"},
		},
	})

	users.Put(documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"id":     {Type: documentstore.DocumentFieldTypeString, Value: "3"},
			"name":   {Type: documentstore.DocumentFieldTypeString, Value: "Charlie"},
			"age":    {Type: documentstore.DocumentFieldTypeString, Value: "35"},
			"status": {Type: documentstore.DocumentFieldTypeString, Value: "active"},
		},
	})

	logger.Info("✓ Added 3 users to the collection")

	// Create indexes
	if err := users.CreateIndex("status"); err != nil {
		log.Fatal(err)
	}
	logger.Info("✓ Created index on 'status' field")

	if err := users.CreateIndex("age"); err != nil {
		log.Fatal(err)
	}
	logger.Info("✓ Created index on 'age' field")

	// Query by status
	logger.Info("--- Query: All active users ---")
	activeStatus := "active"
	results, err := users.Query("status", documentstore.QueryParams{
		MinValue: &activeStatus,
		MaxValue: &activeStatus,
	})
	if err != nil {
		panic(err)
	}
	for _, doc := range results {
		logger.Info(fmt.Sprintf(" - %s (age: %s)", doc.Fields["name"].Value, doc.Fields["age"].Value))
	}

	// Query by age range
	logger.Info("--- Query: Users aged 28-35 ---")
	minAge := "28"
	maxAge := "35"
	results, err = users.Query("age", documentstore.QueryParams{
		MinValue: &minAge,
		MaxValue: &maxAge,
	})
	if err != nil {
		panic(err)
	}
	for _, doc := range results {
		logger.Info(fmt.Sprintf("  - %s (age: %s, status: %s)",
			doc.Fields["name"].Value,
			doc.Fields["age"].Value),
			doc.Fields["status"].Value)
	}

	// Query all users in descending order by age
	logger.Info("--- Query: All users (desc by age) ---")
	results, err = users.Query("age", documentstore.QueryParams{Desc: true})
	if err != nil {
		panic(err)
	}
	for _, doc := range results {
		logger.Info(fmt.Sprintf("  - %s (age: %s)",
			doc.Fields["name"].Value,
			doc.Fields["age"].Value))
	}

	// Dump the store
	logger.Info("--- Dumping store to file ---")
	if err := store.DumpToFile("dump_with_indexes.json"); err != nil {
		log.Fatal(err)
	}
	logger.Info("Store dumped to dump_with_indexes.json")

	// Restore from dump
	logger.Info("--- Restoring store from dump ---")
	restoredStore, err := documentstore.NewStoreFromFile("dump_with_indexes.json")
	if err != nil {
		log.Fatal(err)
	}
	logger.Info("Store restored successfully-")

	// Verify indexes work after restore
	restoredUsers, err := restoredStore.GetCollection("users")
	if err != nil {
		log.Fatal(err)
	}

	logger.Info("--- Query on restored store: Active users ---")
	results, err = restoredUsers.Query("status", documentstore.QueryParams{
		MinValue: &activeStatus,
		MaxValue: &activeStatus,
	})
	if err != nil {
		panic(err)
	}
	for _, doc := range results {
		logger.Info(fmt.Sprintf("  - %s",
			doc.Fields["name"].Value))
	}
	
	logger.Info("All operations completed successfully!")
}
