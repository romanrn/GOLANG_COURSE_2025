package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	cmds "lesson_13/internal/commands"
	"lesson_13/internal/documentstore"
	"log/slog"
	"net"
	"os"
	"strings"
)

var logger *slog.Logger

const (
	defaultPort = "8080"
	defaultHost = "localhost"
)

func getHost() string {
	if host := os.Getenv("SERVER_HOST"); host != "" {
		return host
	}
	return defaultHost
}

func getPort() string {
	if port := os.Getenv("SERVER_PORT"); port != "" {
		return port
	}
	return defaultPort
}

type Server struct {
	listener net.Listener
	store    *documentstore.Store
}

func NewServer() *Server {
	return &Server{
		store: documentstore.NewStore(),
	}
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type Request struct {
	Command    string                 `json:"command"`
	Collection string                 `json:"collection,omitempty"`
	Config     *CollectionConfigJSON  `json:"config,omitempty"`
	Document   map[string]interface{} `json:"document,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Field      string                 `json:"field,omitempty"`
	Value      string                 `json:"value,omitempty"`
}
type CollectionConfigJSON struct {
	PrimaryKey string `json:"primary_key"`
}

func (s *Server) Start(host, port string) error {
	addr := net.JoinHostPort(host, port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("Failed to start listener", "error", err)
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = l

	logger.Info("Server started", "address", addr)
	// Receiving connections
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Error("Failed to accept connection", "error", err)
			return fmt.Errorf("error accepting connection: %w", err)
		}
		logger.Info("New client connected", "remote_addr", conn.RemoteAddr())
		// Handle connection in a new goroutine
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Printf("error closing connection: %v", err)
		}
		logger.Info("Client disconnected", "remote_addr", conn.RemoteAddr())
	}()

	reader := bufio.NewScanner(conn)
	w := bufio.NewWriter(conn)

	for reader.Scan() {
		msg := reader.Text()
		var response interface{}

		req, err := base64.StdEncoding.DecodeString(msg)
		if err != nil {
			logger.Warn("Failed to decode base64 request", "error", err)
			response = Response{Success: false, Message: "Invalid base64 encoding"}
		} else {
			response = s.processRequest(req)
		}
		responseJSON, err := json.Marshal(response)
		if err != nil {
			logger.Error("JSON marshal error", "error", err)
			errorResp := Response{Success: false, Message: fmt.Sprintf("Internal server error: %s\n", err)}
			responseJSON, err = json.Marshal(errorResp)
			if err != nil {
				logger.Error("Failed to marshal error response", "error", err)
				return
			}
		}

		_, err = w.Write(append(responseJSON, '\n'))
		if err != nil {
			logger.Error("Write error", "error", err)
			return
		}
		if err = w.Flush(); err != nil {
			logger.Error("Flush error", "error", err)
			return
		}
	}
}

func (s *Server) processRequest(bReq []byte) Response {
	var req Request
	err := json.Unmarshal(bReq, &req)
	if err != nil {
		return Response{
			Success: false,
			Message: fmt.Sprintf("Invalid JSON: %v", err),
		}
	}

	switch strings.ToUpper(req.Command) {
	case "CREATE_COLLECTION":
		return s.handleCreateCollection(req)
	case "CHECK_COLLECTION":
		return s.handleCheckCollection(req)
	case "DELETE_COLLECTION":
		return s.handleDeleteCollection(req)
	case "PUT":
		return s.handlePutDocument(req)
	case "GET":
		return s.handleGetDocument(req)
	case "LIST":
		return s.handleDocumentsList(req)
	case "DELETE":
		return s.handleDeleteDocument(req)
	case cmds.PingCommandName:
		return Response{Success: true, Message: "PONG"}
	default:
		return Response{
			Success: false,
			Message: fmt.Sprintf("Unknown command: %s", req.Command),
		}
	}
}

func (s *Server) handleCreateCollection(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}

	var cfg *documentstore.CollectionConfig
	if req.Config != nil && req.Config.PrimaryKey != "" {
		cfg = &documentstore.CollectionConfig{
			PrimaryKey: req.Config.PrimaryKey,
		}
	} else {
		cfg = &documentstore.CollectionConfig{
			PrimaryKey: "id",
		}
	}

	_, err := s.store.CreateCollection(req.Collection, cfg)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Failed to create collection: %v", err)}
	}

	return Response{
		Success: true,
		Message: fmt.Sprintf("Collection '%s' created", req.Collection),
	}
}

func (s *Server) handleCheckCollection(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}

	_, err := s.store.GetCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Collection not found: %v", err)}
	}

	return Response{
		Success: true,
		Message: "Collection exists",
	}
}

func (s *Server) handleDeleteCollection(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}

	err := s.store.DeleteCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Failed to delete collection: %v", err)}
	}

	return Response{Success: true, Message: fmt.Sprintf("Collection '%s' deleted", req.Collection)}
}

func (s *Server) handlePutDocument(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}
	if req.Document == nil {
		return Response{Success: false, Message: "Document is required"}
	}

	coll, err := s.store.GetCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Collection not found: %v", err)}
	}

	doc, err := convertToDocument(req.Document)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Invalid document: %v", err)}
	}

	err = coll.Put(*doc)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Failed to put document: %v", err)}
	}

	return Response{Success: true, Message: "Document added"}
}

func (s *Server) handleGetDocument(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}
	if req.Key == "" {
		return Response{Success: false, Message: "Key is required"}
	}

	coll, err := s.store.GetCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Collection not found: %v", err)}
	}

	doc, err := coll.Get(req.Key)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Document not found: %v", err)}
	}

	return Response{Success: true, Data: doc.Fields}
}

func convertToDocument(data map[string]interface{}) (*documentstore.Document, error) {
	fields := make(map[string]documentstore.DocumentField)

	for key, value := range data {
		field, err := convertToDocumentField(value)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", key, err)
		}
		fields[key] = field
	}

	return &documentstore.Document{Fields: fields}, nil
}

func convertToDocumentField(value interface{}) (documentstore.DocumentField, error) {
	switch v := value.(type) {
	case string:
		return documentstore.DocumentField{
			Type:  documentstore.DocumentFieldTypeString,
			Value: v,
		}, nil
	case float64, int, int64, float32:
		return documentstore.DocumentField{
			Type:  documentstore.DocumentFieldTypeNumber,
			Value: v,
		}, nil
	case bool:
		return documentstore.DocumentField{
			Type:  documentstore.DocumentFieldTypeBool,
			Value: v,
		}, nil
	case []interface{}:
		return documentstore.DocumentField{
			Type:  documentstore.DocumentFieldTypeArray,
			Value: v,
		}, nil
	case map[string]interface{}:
		return documentstore.DocumentField{
			Type:  documentstore.DocumentFieldTypeObject,
			Value: v,
		}, nil
	default:
		return documentstore.DocumentField{}, fmt.Errorf("unsupported type: %T", v)
	}
}

func (s *Server) handleDocumentsList(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}

	coll, err := s.store.GetCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Collection not found: %v", err)}
	}

	docs := coll.List()
	result := make([]map[string]documentstore.DocumentField, 0, len(docs))
	for _, doc := range docs {
		result = append(result, doc.Fields)
	}

	return Response{Success: true, Data: result}
}

func (s *Server) handleDeleteDocument(req Request) Response {
	if req.Collection == "" {
		return Response{Success: false, Message: "Collection name is required"}
	}
	if req.Key == "" {
		return Response{Success: false, Message: "Key is required"}
	}

	coll, err := s.store.GetCollection(req.Collection)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Collection not found: %v", err)}
	}

	err = coll.Delete(req.Key)
	if err != nil {
		return Response{Success: false, Message: fmt.Sprintf("Failed to delete document: %v", err)}
	}

	return Response{Success: true, Message: "Document deleted"}
}

func main() {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger = slog.New(h).With(slog.String("component", "main_server")) // ініціалізуємо глобальну змінну

	logger.Info("=== Server ===")
	server := NewServer()

	host := getHost()
	port := getPort()
	logger.Info("Starting TCP server", "host", host, "port", port)
	if err := server.Start(host, port); err != nil {
		logger.Error("Server error", "error", err)
		os.Exit(1)
	}
}
