package main

import (
	"errors"
	"fmt"
	"lesson_06/documentstore"
	"log"
	"log/slog"
	"os"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main"))
	//documentstore.SetLogger(logger)
	logger.Info("=== DocumentStore ===")

	// Create a new document store
	store := documentstore.NewStore()
	//fmt.Println("\n=== Create Users ===")
	usersCollectionPtr, err := store.CreateCollection("users", &documentstore.CollectionConfig{
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

	usersCollectionPtr.Put(doc1)
	usersCollectionPtr.Put(doc2)

	store.DumpToFile("dump1.json")
	store2, err := documentstore.NewStoreFromFile(("dump1.json"))
	if err != nil {
		log.Fatal(err)
	}
	collection2, err := store2.GetCollection("users")
	listDocs := collection2.List()

	logger.Info(fmt.Sprintf("=== Total documents: === '%d' ", len(listDocs)))
	for _, doc := range listDocs {
		fmt.Printf("  - %s: %s\n", doc.Fields["key"].Value, doc.Fields["name"].Value)
	}
}
