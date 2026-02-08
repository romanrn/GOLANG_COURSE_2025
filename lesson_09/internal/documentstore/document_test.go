// go
package documentstore

import (
	"testing"
)

type user struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
	// unexported should be ignored
	secret string
	// skipped by json tag
	SkipMe string `json:"-"`
}

type hasUnsupported struct {
	Ch chan int // unsupported -> should trigger ErrUnsupportedDocumentField
}

func TestMarshalDocument_Struct(t *testing.T) {
	u := user{
		ID:    42,
		Name:  "alice",
		Admin: true,
	}

	doc, err := MarshalDocument(u)
	if err != nil {
		t.Fatalf("MarshalDocument error: %v", err)
	}
	if doc == nil || doc.Fields == nil {
		t.Fatalf("MarshalDocument returned nil document")
	}

	// json tags respected
	if f, ok := doc.Fields["id"]; !ok || f.Type != DocumentFieldTypeNumber || f.Value.(int) != 42 {
		t.Fatalf("id field mismatch: %+v", f)
	}
	if f, ok := doc.Fields["name"]; !ok || f.Type != DocumentFieldTypeString || f.Value.(string) != "alice" {
		t.Fatalf("name field mismatch: %+v", f)
	}
	if f, ok := doc.Fields["admin"]; !ok || f.Type != DocumentFieldTypeBool || f.Value.(bool) != true {
		t.Fatalf("admin field mismatch: %+v", f)
	}

	// unexported skipped
	if _, ok := doc.Fields["secret"]; ok {
		t.Fatalf("unexported field should be skipped")
	}
	// json:"-" skipped
	if _, ok := doc.Fields["SkipMe"]; ok {
		t.Fatalf("json:\"-\" field should be skipped")
	}
}

func TestMarshalDocument_PointerInput(t *testing.T) {
	u := &user{ID: 1, Name: "bob", Admin: false}
	doc, err := MarshalDocument(u)
	if err != nil {
		t.Fatalf("MarshalDocument error: %v", err)
	}
	if doc.Fields["id"].Value.(int) != 1 {
		t.Fatalf("unexpected id value")
	}
	if doc.Fields["name"].Value.(string) != "bob" {
		t.Fatalf("unexpected name value")
	}
}

func TestMarshalDocument_Errors(t *testing.T) {
	// nil input
	if _, err := MarshalDocument(nil); err != ErrDocumentInputNull {
		t.Fatalf("expected ErrDocumentInputNull, got %v", err)
	}
	// non-struct input
	if _, err := MarshalDocument(123); err != ErrDocumentInputIsNotStruct {
		t.Fatalf("expected ErrDocumentInputIsNotStruct, got %v", err)
	}
	// unsupported field type triggers error
	if _, err := MarshalDocument(hasUnsupported{}); err != ErrUnsupportedDocumentField {
		t.Fatalf("expected ErrUnsupportedDocumentField, got %v", err)
	}
}

func TestUnmarshalDocument_HappyPath(t *testing.T) {
	// Prepare a document matching user struct tags
	doc := &Document{
		Fields: map[string]DocumentField{
			"id":    {Type: DocumentFieldTypeNumber, Value: 7},
			"name":  {Type: DocumentFieldTypeString, Value: "carol"},
			"admin": {Type: DocumentFieldTypeBool, Value: true},
		},
	}

	var u user
	if err := UnmarshalDocument(doc, &u); err != nil {
		t.Fatalf("UnmarshalDocument error: %v", err)
	}
	if u.ID != 7 || u.Name != "carol" || !u.Admin {
		t.Fatalf("unexpected struct values: %+v", u)
	}
}

func TestUnmarshalDocument_TypeMismatchIgnored(t *testing.T) {
	doc := &Document{
		Fields: map[string]DocumentField{
			"id":    {Type: DocumentFieldTypeString, Value: "not-a-number"},
			"name":  {Type: DocumentFieldTypeNumber, Value: 123}, // int convertible to string -> "{"
			"admin": {Type: DocumentFieldTypeBool, Value: false},
		},
	}

	var u user
	if err := UnmarshalDocument(doc, &u); err != nil {
		t.Fatalf("UnmarshalDocument error: %v", err)
	}
	// id string should not set int field -> remains default(0)
	if u.ID != 0 {
		t.Fatalf("expected ID to remain default(0), got %d", u.ID)
	}
	// name receives converted string from int (123 -> '{')
	if u.Name != "{" {
		t.Fatalf("expected Name to be '{', got %q", u.Name)
	}
	if u.Admin != false {
		t.Fatalf("expected Admin=false, got %v", u.Admin)
	}
}

func TestUnmarshalDocument_Errors(t *testing.T) {
	// nil doc
	if err := UnmarshalDocument(nil, new(user)); err != ErrUnmarshalDocumentIsNull {
		t.Fatalf("expected ErrUnmarshalDocumentIsNull, got %v", err)
	}
	// doc with nil fields
	if err := UnmarshalDocument(&Document{Fields: nil}, new(user)); err != ErrUnmarshalDocumentIsNull {
		t.Fatalf("expected ErrUnmarshalDocumentIsNull for nil fields, got %v", err)
	}
	// nil output pointer
	doc := &Document{Fields: map[string]DocumentField{}}
	if err := UnmarshalDocument(doc, nil); err != ErrUnmarshalOutputIsNull {
		t.Fatalf("expected ErrUnmarshalOutputIsNull, got %v", err)
	}
	// non-pointer output
	var u user
	if err := UnmarshalDocument(doc, u); err != ErrUnmarshalOutputIsNull {
		t.Fatalf("expected ErrUnmarshalOutputIsNull for non-pointer, got %v", err)
	}
	// pointer to non-struct (e.g., *int)
	var x int
	if err := UnmarshalDocument(doc, &x); err != ErrUnmarshalOutputIsNotStruct {
		t.Fatalf("expected ErrUnmarshalOutputIsNotStruct, got %v", err)
	}
}
