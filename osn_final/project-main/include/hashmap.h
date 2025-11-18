#ifndef HASHMAP_H
#define HASHMAP_H

#include "common.h"

#define HASHMAP_SIZE 1024

typedef struct HashNode {
    char key[MAX_FILENAME];
    void* value;
    struct HashNode* next;
} HashNode;

typedef struct {
    HashNode* buckets[HASHMAP_SIZE];
    pthread_rwlock_t lock;
    int size;
} HashMap;

// HashMap functions
HashMap* hashmap_create();
void hashmap_destroy(HashMap* map);
unsigned int hash(const char* key);
void hashmap_put(HashMap* map, const char* key, void* value);
void* hashmap_get(HashMap* map, const char* key);
void hashmap_remove(HashMap* map, const char* key);
int hashmap_contains(HashMap* map, const char* key);
void hashmap_get_keys(HashMap* map, char keys[][MAX_FILENAME], int* count);

// LRU Cache
typedef struct LRUNode {
    char key[MAX_FILENAME];
    void* value;
    struct LRUNode* prev;
    struct LRUNode* next;
} LRUNode;

typedef struct {
    LRUNode* head;
    LRUNode* tail;
    HashMap* map;
    int capacity;
    int size;
    pthread_mutex_t lock;
} LRUCache;

// LRU Cache functions
LRUCache* lru_create(int capacity);
void lru_destroy(LRUCache* cache);
void* lru_get(LRUCache* cache, const char* key);
void lru_put(LRUCache* cache, const char* key, void* value);
void lru_remove(LRUCache* cache, const char* key);

#endif // HASHMAP_H
