#ifndef FLEXQL_CACHE_H
#define FLEXQL_CACHE_H

#include <stddef.h>
#include <pthread.h>

/* ── LRU Query Result Cache ───────────────────────────────── */
#define CACHE_CAPACITY 512   /* max cached queries */
#define CACHE_KEY_MAX  1024  /* max SQL key length */

typedef struct CacheRow {
    int    ncols;
    char **values;           /* ncols strings */
    char **col_names;        /* ncols strings */
    struct CacheRow *next;
} CacheRow;

typedef struct CacheEntry {
    char        key[CACHE_KEY_MAX];
    CacheRow   *rows;
    int         nrows;
    /* LRU list pointers */
    struct CacheEntry *prev;
    struct CacheEntry *next;
    /* hash chain */
    struct CacheEntry *hash_next;
} CacheEntry;

typedef struct {
    CacheEntry *buckets[CACHE_CAPACITY * 2];
    CacheEntry *lru_head;   /* most recently used */
    CacheEntry *lru_tail;   /* least recently used */
    int         size;
    int         capacity;
    pthread_mutex_t lock;
} LRUCache;

LRUCache   *cache_create(int capacity);
void        cache_destroy(LRUCache *c);
CacheEntry *cache_get(LRUCache *c, const char *key);
void        cache_put(LRUCache *c, const char *key, CacheRow *rows, int nrows);
void        cache_invalidate(LRUCache *c, const char *table_name);

#endif /* FLEXQL_CACHE_H */
