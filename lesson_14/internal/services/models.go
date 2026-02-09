package services

import "go.mongodb.org/mongo-driver/bson/primitive"

type Collection struct {
	ID   int
	Name string
}

type Document struct {
	ID   primitive.ObjectID     `bson:"_id,omitempty"`
	Data map[string]interface{} `bson:",inline"`
}
