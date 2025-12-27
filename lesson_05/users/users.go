package users

import (
	"errors"
	"lesson_05/documentstore"
)

var (
	ErrUserNotFound     = errors.New("user not found")
	ErrDeleteUser       = errors.New("the Error user deletion")
	ErrUserIdRequired   = errors.New("id is required")
	ErrUserNameRequired = errors.New("name is required")
	ErrUserAgePoistive  = errors.New("age must be positive")
)

type UserError struct {
	Kind error // ErrUserNotFound, ErrDeleteUser, ...
	Err  error // reason
}

func (e *UserError) Error() string {
	return e.Kind.Error()
}

func (e *UserError) Unwrap() error {
	return e.Err
}

func (e *UserError) Is(target error) bool {
	return target == e.Kind
}

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Service struct {
	coll *documentstore.Collection
}

// NewService creates a users service backed by a documentstore collection.
func NewService(coll *documentstore.Collection) *Service {
	return &Service{coll: coll}
}

func (s *Service) CreateUser(id, name string, age int) (*User, error) {
	// ...
	if id == "" {
		return nil, ErrUserIdRequired
	}
	if name == "" {
		return nil, ErrUserNameRequired
	}
	if age <= 0 {
		return nil, ErrUserAgePoistive
	}

	user := &User{
		ID:   id,
		Name: name,
		Age:  age,
	}

	doc, err := documentstore.MarshalDocument(user)
	if err != nil {
		return nil, err
	}

	s.coll.Put(*doc)

	return user, nil

}

func (s *Service) ListUsers() ([]User, error) {
	// ...
	docs := s.coll.List()
	users := make([]User, 0, len(docs))
	for _, docVal := range docs {
		var user User
		doc := &docVal
		if err := documentstore.UnmarshalDocument(doc, &user); err != nil {
			return nil, err
		}
		users = append(users, user)
	}

	return users, nil
}

func (s *Service) GetUser(userID string) (*User, error) {
	// ...
	doc, err := s.coll.Get(userID)
	if err != nil {
		return nil, &UserError{
			//Err: err, // ErrDocumentNotFound inside
			Kind: ErrUserNotFound,
			Err:  err,
		}
	}

	var user User
	if err := documentstore.UnmarshalDocument(doc, &user); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *Service) DeleteUser(userID string) error {
	// ...
	if err := s.coll.Delete(userID); err != nil {
		return &UserError{
			// Err: err, // ErrDocumentNotFound inside
			Kind: ErrDeleteUser,
			Err:  err,
		}
	}
	return nil
}
