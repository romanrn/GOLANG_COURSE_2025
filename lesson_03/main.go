package main

import (
	"fmt"
	"lesson_03/documentstore"
)

func main() {
	fmt.Println("### START")

	// 1.Testing invalid document (missing key):
	fmt.Println("\n1. Testing invalid document (missing key):")
	invalidDoc := &documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"name": {Type: documentstore.DocumentFieldTypeString, Value: "Test"},
		},
	}
	documentstore.Put(invalidDoc)

	// 2. Testing invalid document (key is number)
	fmt.Println("\n2. Testing invalid document (key is number):")
	invalidKeyDoc := &documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"key": {Type: documentstore.DocumentFieldTypeNumber, Value: 123},
		},
	}
	documentstore.Put(invalidKeyDoc)

	// 3. Testing valid document:
	fmt.Println("\n3. Testing valid document:")
	validDoc := &documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"key": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "user_100"},
			"email": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "test@test.com"},
			"age": {
				Type:  documentstore.DocumentFieldTypeNumber,
				Value: 25},
			"is_active": {
				Type:  documentstore.DocumentFieldTypeBool,
				Value: true},
			"roles": {
				Type:  documentstore.DocumentFieldTypeArray,
				Value: []string{"admin", "editor", "viewer"}},
		},
	}
	documentstore.Put(validDoc)

	// 4. Get document by key
	fmt.Println("\n4. Testing Get 'user_100':")
	doc, found := documentstore.Get("user_100")
	if found {
		email := doc.Fields["email"].Value
		fmt.Printf("Found document! Email: %v\n", email)
	} else {
		fmt.Println("Document not found.")
	}

	// 5. Testing List
	fmt.Println("\n5. Testing List:")
	// Add one more document
	documentstore.Put(&documentstore.Document{
		Fields: map[string]documentstore.DocumentField{
			"key": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "user_101"},
			"email": {
				Type:  documentstore.DocumentFieldTypeString,
				Value: "test2@test.com"},
			"age": {
				Type:  documentstore.DocumentFieldTypeNumber,
				Value: 35},
			"is_active": {
				Type:  documentstore.DocumentFieldTypeBool,
				Value: false},
			"roles": {
				Type:  documentstore.DocumentFieldTypeArray,
				Value: []string{"editor", "viewer"}},
		},
	})

	list := documentstore.List()
	fmt.Printf("Count of documents in store: %d\n", len(list))
	for i, d := range list {
		fmt.Printf(" - Doc #%d ; Key: %v\n", i+1, d.Fields["key"].Value)
	}

	// 6. Testing Delete
	fmt.Println("\n6. Testing Delete 'user_100':")
	deleted := documentstore.Delete("user_100")
	if deleted {
		fmt.Println("Document 'user_100' deleted successfully.")
	} else {
		fmt.Println("Failed to delete.")
	}

	// check if deleted
	_, foundAfterDelete := documentstore.Get("user_100")
	if !foundAfterDelete {
		fmt.Println("Verification: 'user_100' is strictly gone.")
	}

	// Check remaining documents
	fmt.Printf("Documents remaining: %d\n", len(documentstore.List()))

	fmt.Println("\n### END TEST")
}
