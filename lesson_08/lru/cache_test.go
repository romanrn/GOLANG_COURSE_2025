package lru

import (
	"testing"
)

func TestLruCache_PutAndGet(t *testing.T) {
	cache := NewLruCache(2)

	// Перевіряємо додавання і отримання значень
	cache.Put("key1", "value1")
	if value, ok := cache.Get("key1"); !ok || value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	cache.Put("key2", "value2")
	if value, ok := cache.Get("key2"); !ok || value != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value)
	}

	// Перевіряємо, що значення зберігаються правильно
	if value, ok := cache.Get("key1"); !ok || value != "value1" {
		t.Errorf("Expected 'value1' after adding 'key2', got '%s'", value)
	}
}

func TestLruCache_ReplaceLeastRecentlyUsed(t *testing.T) {
	cache := NewLruCache(2)

	// Додаємо два елементи
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	// Досягаємо ліміту кешу
	cache.Put("key3", "value3")

	// Перевіряємо, що перший доданий елемент було видалено
	if _, ok := cache.Get("key1"); ok {
		t.Errorf("Expected 'key1' to be evicted")
	}

	// Перевіряємо, що інші елементи ще є в кеші
	if value, ok := cache.Get("key2"); !ok || value != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value)
	}

	if value, ok := cache.Get("key3"); !ok || value != "value3" {
		t.Errorf("Expected 'value3', got '%s'", value)
	}
}

func TestLruCache_UpdateKey(t *testing.T) {
	cache := NewLruCache(2)

	// Додаємо елемент і оновлюємо його значення
	cache.Put("key1", "value1")
	cache.Put("key1", "updated_value1")

	// Перевіряємо, що значення оновлено
	if value, ok := cache.Get("key1"); !ok || value != "updated_value1" {
		t.Errorf("Expected 'updated_value1', got '%s'", value)
	}
}

func TestLruCache_LRUBehavior(t *testing.T) {
	cache := NewLruCache(2)

	// Додаємо три елементи, перевіряємо видалення найстарішого
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Get("key1")           // key1 тепер найновіший
	cache.Put("key3", "value3") // key2 має бути видалений

	if _, ok := cache.Get("key2"); ok {
		t.Errorf("Expected 'key2' to be evicted")
	}

	// Перевіряємо, що key1 та key3 залишились
	if value, ok := cache.Get("key1"); !ok || value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	if value, ok := cache.Get("key3"); !ok || value != "value3" {
		t.Errorf("Expected 'value3', got '%s'", value)
	}
}
