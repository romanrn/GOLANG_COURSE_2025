package main

import (
	"lesson_08/lru"
	"log/slog"
	"os"
)

func main() {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main"))

	logger.Info("=== Start ===")

	cache := lru.NewLruCache(2)
	cache.Put("a", "1")
	cache.Put("b", "2")

	key := "a"
	value, ok := cache.Get(key) // "1", true
	if ok {
		logger.Info("Cache hit", slog.String("key", key), slog.String("value", value))
	} else {
		logger.Info("Cache miss", slog.String("key", key))
	}
	cache.Put("c", "3") // evict "b"

	key = "b"
	value, ok = cache.Get("b") // false
	if ok {
		logger.Info("Cache hit", slog.String("key", key), slog.String("value", value))
	} else {
		logger.Info("Cache miss", slog.String("key", key))
	}
}
