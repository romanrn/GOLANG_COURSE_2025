package lru

type LruCache interface {
	// Якщо наш кеш вже повний (ми досягли нашого capacity)
	// то має видалитись той елемент, який ми до якого ми доступались (читали) найдавніше
	Put(key, value string)
	Get(key string) (string, bool)
}

type node struct {
	key        string
	value      string
	prev, next *node
}

type lruCache struct {
	capacity int
	items    map[string]*node
	head     *node // most recently used
	tail     *node // least recently used
}

func NewLruCache(capacity int) LruCache {
	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*node),
		head:     head,
		tail:     tail,
	}
}

// move to the head (most recently used position)
func (cache *lruCache) moveToHead(n *node) {
	if n == cache.head {
		return // it is head
	}

	cache.remove(n)
	cache.setHead(n)
}

func (cache *lruCache) remove(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
}

func (cache *lruCache) setHead(n *node) {
	n.prev = cache.head
	n.next = cache.head.next
	cache.head.next.prev = n
	cache.head.next = n
}

func (cache *lruCache) evict() {
	prevNode := cache.tail.prev
	cache.remove(prevNode)
	delete(cache.items, prevNode.key)
}

func (cache *lruCache) Get(key string) (string, bool) {
	n, exists := cache.items[key]
	if !exists {
		return "", false
	}

	cache.moveToHead(n)
	return n.value, true
}

func (cache *lruCache) Put(key, value string) {
	if n, ok := cache.items[key]; ok {
		n.value = value
		cache.moveToHead(n)
		return
	}

	if len(cache.items) >= cache.capacity {
		cache.evict()
	}

	n := &node{key: key, value: value}
	cache.items[key] = n
	cache.setHead(n)
}
