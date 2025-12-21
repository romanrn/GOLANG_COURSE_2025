package main

import (
	"fmt"
	"lesson_04/documentstore"
)

func main() {
	fmt.Println("=== DocumentStore ===\n")

	// Create a new document store
	store := documentstore.NewStore()
	fmt.Println("\n=== Create Users Collection ===")
	// Create users collection users with primary key "key"
	created, usersCollection := store.CreateCollection("users", &documentstore.CollectionConfig{
		PrimaryKey: "key",
	})
	fmt.Printf("Users collection created: %t\n\n", created)

	if usersCollection != nil {
		// Add docs
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

		usersCollection.Put(doc1)
		usersCollection.Put(doc2)

		// Get document
		fmt.Println("\n=== Get Document by key: user1===")
		if doc, ok := usersCollection.Get("user1"); ok {
			fmt.Printf("Found document: %+v\n", doc.Fields["name"].Value)
		}

		// Get all documents
		fmt.Println("\n=== List All Documents ===")
		docs := usersCollection.List()
		fmt.Printf("Total documents: %d\n", len(docs))
		for _, doc := range docs {
			fmt.Printf("  - %s: %s\n", doc.Fields["key"].Value, doc.Fields["name"].Value)
		}

		// Delete document
		fmt.Println("\n=== Delete Document by key: user1 ===")
		deleted := usersCollection.Delete("user1")
		fmt.Printf("Document deleted: %t\n", deleted)

		// Get all documents
		fmt.Println("\n=== List After Delete ===")
		docs = usersCollection.List()
		fmt.Printf("Total documents: %d\n", len(docs))
		for _, doc := range docs {
			fmt.Printf("  - %s: %s\n", doc.Fields["key"].Value, doc.Fields["name"].Value)
		}
	}

	// Create product collection
	fmt.Println("\n=== Create Products Collection ===")
	created, productsCollection := store.CreateCollection("products", &documentstore.CollectionConfig{
		PrimaryKey: "id",
	})
	fmt.Printf("Products collection created: %t\n", created)

	if productsCollection != nil {
		product := documentstore.Document{
			Fields: map[string]documentstore.DocumentField{
				"id": {
					Type:  documentstore.DocumentFieldTypeString,
					Value: "prod1",
				},
				"title": {
					Type:  documentstore.DocumentFieldTypeString,
					Value: "Laptop",
				},
				"price": {
					Type:  documentstore.DocumentFieldTypeNumber,
					Value: 999.99,
				},
			},
		}
		productsCollection.Put(product)
	}

	// Create collection duplicate
	fmt.Println("\n=== Try to Create Duplicate Collection ===")
	created, _ = store.CreateCollection("users", &documentstore.CollectionConfig{
		PrimaryKey: "key",
	})
	fmt.Printf("Duplicate collection created: %t\n", created)

	// Get collections

	getCollection(store, "users")
	getCollection(store, "products")

	// Delete collection
	fmt.Println("\n=== Delete products Collection ===")
	deleted := store.DeleteCollection("products")
	fmt.Printf("Products collection deleted: %t\n", deleted)

	getCollection(store, "products")

	fmt.Println("\n=== The End ===")
}

func getCollection(store *documentstore.Store, name string) {
	fmt.Printf("\n=== Get %s Collection ===", name)
	col, ok := store.GetCollection(name)
	if ok {
		docs := col.List()
		fmt.Printf("%s  collection found with %d documents\n", name, len(docs))
	}
}
