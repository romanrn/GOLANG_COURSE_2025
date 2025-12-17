package documentstore

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrDocumentInputNull          = errors.New("input is nil")
	ErrDocumentInputIsNotStruct   = errors.New("input must be struct")
	ErrUnmarshalDocumentIsNull    = errors.New("unmarshal: document is nil")
	ErrUnmarshalOutputIsNull      = errors.New("unmarshal: expected non-nil pointer")
	ErrUnmarshalOutputIsNotStruct = errors.New("unmarshal: expected struct pointer")
	ErrUnsupportedDocumentField   = errors.New("unsupported document field")
)

type DocumentFieldType string

const (
	DocumentFieldTypeString DocumentFieldType = "string"
	DocumentFieldTypeNumber DocumentFieldType = "number"
	DocumentFieldTypeBool   DocumentFieldType = "bool"
	DocumentFieldTypeArray  DocumentFieldType = "array"
	DocumentFieldTypeObject DocumentFieldType = "object"
)

type DocumentField struct {
	Type  DocumentFieldType
	Value interface{}
}
type Document struct {
	Fields map[string]DocumentField
}

func MarshalDocument(input any) (*Document, error) {
	if input == nil {
		return nil, ErrDocumentInputNull
	}

	v := reflect.ValueOf(input)
	// Check for pointer and get the element
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	// Check if it's struct
	if v.Kind() != reflect.Struct {
		return nil, ErrDocumentInputIsNotStruct
	}

	doc := &Document{
		Fields: make(map[string]DocumentField),
	}

	// get type info
	typ := v.Type()
	// handle all struct fields
	for i := 0; i < v.NumField(); i++ {
		// get field type info
		fieldType := typ.Field(i)

		if !fieldType.IsExported() {
			continue
		}

		name := fieldType.Name
		value := v.Field(i)

		// Check for json tag
		if tag := fieldType.Tag.Get("json"); tag != "" {

			name = strings.Split(tag, ",")[0]
			if name == "-" {
				continue
			}
		}

		// Determine DocumentFieldType
		var docType DocumentFieldType
		switch value.Kind() {
		case reflect.String:
			docType = DocumentFieldTypeString
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			docType = DocumentFieldTypeNumber
		case reflect.Bool:
			docType = DocumentFieldTypeBool
		case reflect.Slice, reflect.Array:
			docType = DocumentFieldTypeArray
		case reflect.Map, reflect.Struct:
			docType = DocumentFieldTypeObject
		default:
			// Skip unsupported types
			fmt.Printf("Skipping field '%s': unsupported type '%s'\n", name, value.Kind())
			return nil, ErrUnsupportedDocumentField
			//continue
		}

		// map[string]DocumentField
		doc.Fields[name] = DocumentField{
			Type:  docType,
			Value: value.Interface(), // Return value as any
		}
	}

	return doc, nil
}

func UnmarshalDocument(doc *Document, output any) error {

	if doc == nil || doc.Fields == nil {
		return ErrUnmarshalDocumentIsNull
	}

	val := reflect.ValueOf(output)

	// is output pointer
	if val.Kind() != reflect.Pointer || val.IsNil() {
		return ErrUnmarshalOutputIsNull
	}

	// Get element
	val = val.Elem()

	// Check struct
	if val.Kind() != reflect.Struct {
		return ErrUnmarshalOutputIsNotStruct
	}

	// Get type info
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		fieldType := typ.Field(i)

		if !fieldVal.CanSet() {
			continue // private field
		}

		// Get field name in a document by json tag or field name
		name := fieldType.Name
		if tag := fieldType.Tag.Get("json"); tag != "" {
			name = strings.Split(tag, ",")[0]
		}
		// Doc has field
		if docField, ok := doc.Fields[name]; ok {
			// Get doc field value
			storedVal := reflect.ValueOf(docField.Value)

			// cast to appropriate type
			if storedVal.Type().ConvertibleTo(fieldVal.Type()) {
				fieldVal.Set(storedVal.Convert(fieldVal.Type()))
			} else {
				// Skip incompatible types
				fmt.Printf("Warning: type mismatch for field %s\n", name)
			}
		}
	}

	return nil
}
