#include "../include/hashmap.h"

// Hash function (djb2)
unsigned int hash(const char* key) {
    unsigned int hash = 5381;
    int c;
    
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    
    return hash % HASHMAP_SIZE;
}

HashMap* hashmap_create() {
    HashMap* map = (HashMap*)malloc(sizeof(HashMap));
    if (!map) return NULL;
    
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        map->buckets[i] = NULL;
    }
    
    pthread_rwlock_init(&map->lock, NULL);
    map->size = 0;
    
    return map;
}

void hashmap_destroy(HashMap* map) {
    if (!map) return;
    
    pthread_rwlock_wrlock(&map->lock);
    
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashNode* node = map->buckets[i];
        while (node) {
            HashNode* next = node->next;
            free(node->value);
            free(node);
            node = next;
        }
    }
    
    pthread_rwlock_unlock(&map->lock);
    pthread_rwlock_destroy(&map->lock);
    free(map);
}

void hashmap_put(HashMap* map, const char* key, void* value) {
    if (!map || !key) return;
    
    unsigned int index = hash(key);
    
    pthread_rwlock_wrlock(&map->lock);
    
    HashNode* node = map->buckets[index];
    
    // Check if key already exists
    while (node) {
        if (strcmp(node->key, key) == 0) {
            // Update existing value
            free(node->value);
            node->value = value;
            pthread_rwlock_unlock(&map->lock);
            return;
        }
        node = node->next;
    }
    
    // Create new node
    HashNode* new_node = (HashNode*)malloc(sizeof(HashNode));
    strncpy(new_node->key, key, MAX_FILENAME - 1);
    new_node->key[MAX_FILENAME - 1] = '\0';
    new_node->value = value;
    new_node->next = map->buckets[index];
    map->buckets[index] = new_node;
    map->size++;
    
    pthread_rwlock_unlock(&map->lock);
}

void* hashmap_get(HashMap* map, const char* key) {
    if (!map || !key) return NULL;
    
    unsigned int index = hash(key);
    
    pthread_rwlock_rdlock(&map->lock);
    
    HashNode* node = map->buckets[index];
    
    while (node) {
        if (strcmp(node->key, key) == 0) {
            void* value = node->value;
            pthread_rwlock_unlock(&map->lock);
            return value;
        }
        node = node->next;
    }
    
    pthread_rwlock_unlock(&map->lock);
    return NULL;
}

void hashmap_remove(HashMap* map, const char* key) {
    if (!map || !key) return;
    
    unsigned int index = hash(key);
    
    pthread_rwlock_wrlock(&map->lock);
    
    HashNode* node = map->buckets[index];
    HashNode* prev = NULL;
    
    while (node) {
        if (strcmp(node->key, key) == 0) {
            if (prev) {
                prev->next = node->next;
            } else {
                map->buckets[index] = node->next;
            }
            
            free(node->value);
            free(node);
            map->size--;
            pthread_rwlock_unlock(&map->lock);
            return;
        }
        
        prev = node;
        node = node->next;
    }
    
    pthread_rwlock_unlock(&map->lock);
}

int hashmap_contains(HashMap* map, const char* key) {
    return hashmap_get(map, key) != NULL;
}

void hashmap_get_keys(HashMap* map, char keys[][MAX_FILENAME], int* count) {
    if (!map || !keys || !count) return;
    
    *count = 0;
    
    pthread_rwlock_rdlock(&map->lock);
    
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashNode* node = map->buckets[i];
        while (node) {
            strncpy(keys[*count], node->key, MAX_FILENAME - 1);
            keys[*count][MAX_FILENAME - 1] = '\0';
            (*count)++;
            node = node->next;
        }
    }
    
    pthread_rwlock_unlock(&map->lock);
}

// LRU Cache implementation
LRUCache* lru_create(int capacity) {
    LRUCache* cache = (LRUCache*)malloc(sizeof(LRUCache));
    if (!cache) return NULL;
    
    cache->head = NULL;
    cache->tail = NULL;
    cache->map = hashmap_create();
    cache->capacity = capacity;
    cache->size = 0;
    pthread_mutex_init(&cache->lock, NULL);
    
    return cache;
}

void lru_destroy(LRUCache* cache) {
    if (!cache) return;
    
    pthread_mutex_lock(&cache->lock);
    
    LRUNode* node = cache->head;
    while (node) {
        LRUNode* next = node->next;
        free(node->value);
        free(node);
        node = next;
    }
    
    hashmap_destroy(cache->map);
    
    pthread_mutex_unlock(&cache->lock);
    pthread_mutex_destroy(&cache->lock);
    free(cache);
}

static void lru_move_to_front(LRUCache* cache, LRUNode* node) {
    if (cache->head == node) return;
    
    // Remove from current position
    if (node->prev) {
        node->prev->next = node->next;
    }
    if (node->next) {
        node->next->prev = node->prev;
    }
    if (cache->tail == node) {
        cache->tail = node->prev;
    }
    
    // Move to front
    node->prev = NULL;
    node->next = cache->head;
    
    if (cache->head) {
        cache->head->prev = node;
    }
    
    cache->head = node;
    
    if (!cache->tail) {
        cache->tail = node;
    }
}

static void lru_remove_tail(LRUCache* cache) {
    if (!cache->tail) return;
    
    LRUNode* node = cache->tail;
    
    if (cache->tail->prev) {
        cache->tail->prev->next = NULL;
    }
    cache->tail = cache->tail->prev;
    
    if (cache->head == node) {
        cache->head = NULL;
    }
    
    hashmap_remove(cache->map, node->key);
    free(node->value);
    free(node);
    cache->size--;
}

void* lru_get(LRUCache* cache, const char* key) {
    if (!cache || !key) return NULL;
    
    pthread_mutex_lock(&cache->lock);
    
    LRUNode* node = (LRUNode*)hashmap_get(cache->map, key);
    
    if (node) {
        lru_move_to_front(cache, node);
        void* value = node->value;
        pthread_mutex_unlock(&cache->lock);
        return value;
    }
    
    pthread_mutex_unlock(&cache->lock);
    return NULL;
}

void lru_put(LRUCache* cache, const char* key, void* value) {
    if (!cache || !key) return;
    
    pthread_mutex_lock(&cache->lock);
    
    LRUNode* existing = (LRUNode*)hashmap_get(cache->map, key);
    
    if (existing) {
        // Update existing node
        free(existing->value);
        existing->value = value;
        lru_move_to_front(cache, existing);
        pthread_mutex_unlock(&cache->lock);
        return;
    }
    
    // Create new node
    LRUNode* node = (LRUNode*)malloc(sizeof(LRUNode));
    strncpy(node->key, key, MAX_FILENAME - 1);
    node->key[MAX_FILENAME - 1] = '\0';
    node->value = value;
    node->prev = NULL;
    node->next = cache->head;
    
    if (cache->head) {
        cache->head->prev = node;
    }
    cache->head = node;
    
    if (!cache->tail) {
        cache->tail = node;
    }
    
    hashmap_put(cache->map, key, node);
    cache->size++;
    
    // Remove oldest if over capacity
    if (cache->size > cache->capacity) {
        lru_remove_tail(cache);
    }
    
    pthread_mutex_unlock(&cache->lock);
}

void lru_remove(LRUCache* cache, const char* key) {
    if (!cache || !key) return;
    
    pthread_mutex_lock(&cache->lock);
    
    LRUNode* node = (LRUNode*)hashmap_get(cache->map, key);
    
    if (node) {
        if (node->prev) {
            node->prev->next = node->next;
        } else {
            cache->head = node->next;
        }
        
        if (node->next) {
            node->next->prev = node->prev;
        } else {
            cache->tail = node->prev;
        }
        
        hashmap_remove(cache->map, key);
        free(node->value);
        free(node);
        cache->size--;
    }
    
    pthread_mutex_unlock(&cache->lock);
}
