package main

import (
	"fmt"
	"lesson_11/internal/documentstore"
	"log/slog"
	"os"
	"sync"
)

func main() {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main"))

	logger.Info("=== DocumentStore ===")

	store := documentstore.NewStore()
	cfg := &documentstore.CollectionConfig{PrimaryKey: "id"}
	collection, _ := store.CreateCollection("users", cfg)

	var wg sync.WaitGroup

	numGoroutines := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			logger.Info("goroutine started", slog.Int("goroutine_id", i))
			key := fmt.Sprintf("user_%d", i)

			// Create document
			doc := documentstore.Document{
				Fields: map[string]documentstore.DocumentField{
					"id":   {Type: documentstore.DocumentFieldTypeString, Value: key},
					"name": {Type: documentstore.DocumentFieldTypeString, Value: fmt.Sprintf("User %d", i)},
				},
			}

			// Insert
			collection.Put(doc)

			// Get
			collection.Get(key)

			// Delete
			collection.Delete(key)
		}()
	}

	// wait for all goroutines to finish
	wg.Wait()
	logger.Info("✓ All 1000 goroutines completed")
}
