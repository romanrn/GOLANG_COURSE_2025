package main

import (
	"errors"
	"fmt"
	"lesson_05/documentstore"
	"lesson_05/users"
	"log"
)

func main() {
	fmt.Println("=== DocumentStore ===\n")

	// Create a new document store
	store := documentstore.NewStore()
	fmt.Println("\n=== Create Users ===")
	usersCollectionPtr, err := store.CreateCollection("users", &documentstore.CollectionConfig{
		PrimaryKey: "id",
	})
	switch {
	case err == nil:
		fmt.Println("Created collection 'users'")
	case errors.Is(err, documentstore.ErrCollectionAlreadyExists):
		log.Fatal(err)
	case errors.Is(err, documentstore.ErrCollectionInvalidNameOrKey):
		log.Fatal(err)
	default:
		log.Fatal(err)
	}

	userService := users.NewService(usersCollectionPtr)

	user1, err := userService.CreateUser("user1", "John Doe", 30)
	if err != nil {
		log.Fatal(err)
	}
	user2, err := userService.CreateUser("user2", "Jane Smith", 25)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created users:")
	fmt.Printf("%+v\n", user1)
	fmt.Printf("%+v\n", user2)

	// ListUsers
	list, err := userService.ListUsers()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nAll users:")
	for _, u := range list {
		fmt.Printf("%+v\n", u)
	}

	// 6️⃣ GetUser
	user, err := userService.GetUser("user1")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nGet user u1:")
	fmt.Printf("%+v\n", user)

	// DeleteUser

	if err := userService.DeleteUser("user1"); errors.Is(err, users.ErrDeleteUser) {
		log.Fatal(err)
	}

	fmt.Println("\nUser u1 deleted")

	// Check deletion
	_, err = userService.GetUser("user1")
	if errors.Is(err, users.ErrUserNotFound) {
		fmt.Println("Confirmed: user u1 not found")
	}

	if err := userService.DeleteUser("user1"); errors.Is(err, users.ErrDeleteUser) {
		if errors.Is(err, documentstore.ErrDocumentNotFound) {
			fmt.Println("Confirmed: document not found")
		}
	}
}
