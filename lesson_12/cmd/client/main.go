package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
)

var logger *slog.Logger

const (
	port = "8080"
	host = "localhost"
)

type Request struct {
	Command    string                 `json:"command"`
	Collection string                 `json:"collection,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Document   map[string]interface{} `json:"document,omitempty"`
	Config     *CollectionConfig      `json:"config,omitempty"`
	Field      string                 `json:"field,omitempty"`
	Value      string                 `json:"value,omitempty"`
}

type CollectionConfig struct {
	PrimaryKey string `json:"primary_key"`
}

type Client struct {
	conn net.Conn
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (cl *Client) Start(host, port string) error {
	us := bufio.NewScanner(os.Stdin)
	uw := bufio.NewWriter(os.Stdout)

	addr := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logger.Error("failed dial ", "error", err)
		return fmt.Errorf("failed dial  %s: %w", addr, err)
	}
	printHelp(uw)
	cl.conn = conn
	cw := bufio.NewWriter(conn)
	cr := bufio.NewReader(conn)

	for us.Scan() {
		line := strings.TrimSpace(us.Text())

		if line == "" {
			continue
		}

		if line == "exit" || line == "quit" {
			logger.Info("Exiting...")
			break
		}

		if line == "help" {
			printHelp(uw)
			continue
		}

		encoded := base64.StdEncoding.EncodeToString([]byte(line))
		_, _ = cw.WriteString(encoded + "\n")

		_ = cw.Flush()

		resp, _ := cr.ReadString('\n')
		_, _ = uw.WriteString(resp + "\n")
		_ = uw.Flush()
	}
	return nil
}

func main() {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger = slog.New(h).With(slog.String("component", "main_client"))

	logger.Info("=== Client ===")
	client := NewClient()

	logger.Info(fmt.Sprintf("Starting TCP client for %s : %s)", host, port))
	if err := client.Start(host, port); err != nil {
		logger.Error(fmt.Sprintf("Client error: %v", err))
		os.Exit(1)
	}
	defer client.Close()

}

func printHelp(w *bufio.Writer) {
	help := `
=== Document Store Client ===

- help  : show help - Example of commands
- exit  : exit the client

Example commands (in JSON format):

1. Check connection:
   {"command":"PING"}

2. Create Collection:
   {"command":"CREATE_COLLECTION","collection":"users","config":{"primary_key":"id"}}

3. Add document:
   {"command":"PUT","collection":"users","document":{"id":"u1","name":"Alice","age":30}}

4. Get document:
   {"command":"GET","collection":"users","key":"u1"}

5. List of documents:
   {"command":"LIST","collection":"users"}

6. Delete Document:
   {"command":"DELETE","collection":"users","key":"u1"}

9. Delete collection:
   {"command":"DELETE_COLLECTION","collection":"users"}

10. Check Collection:
    {"command":"CHECK_COLLECTION","collection":"users"}
`
	_, _ = w.WriteString(help)
	_ = w.Flush()
}
