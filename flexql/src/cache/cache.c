/*
 * cache.c – LRU query result cache.
 */

#include "../../include/cache/cache.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static unsigned int cache_hash(const char *key, int cap) {
    unsigned int h = 2166136261u;
    for (; *key; key++) { h ^= (unsigned char)*key; h *= 16777619u; }
    return h % (unsigned int)(cap * 2);
}

LRUCache *cache_create(int capacity) {
    LRUCache *c = calloc(1, sizeof(LRUCache));
    c->capacity = capacity;
    pthread_mutex_init(&c->lock, NULL);
    return c;
}

static void free_cache_rows(CacheRow *r) {
    while (r) {
        CacheRow *nxt = r->next;
        for (int i = 0; i < r->ncols; i++) {
            free(r->values[i]);
            free(r->col_names[i]);
        }
        free(r->values);
        free(r->col_names);
        free(r);
        r = nxt;
    }
}

static void lru_remove(LRUCache *c, CacheEntry *e) {
    if (e->prev) e->prev->next = e->next;
    else c->lru_head = e->next;
    if (e->next) e->next->prev = e->prev;
    else c->lru_tail = e->prev;
    e->prev = e->next = NULL;
}

static void lru_push_front(LRUCache *c, CacheEntry *e) {
    e->next = c->lru_head;
    e->prev = NULL;
    if (c->lru_head) c->lru_head->prev = e;
    else c->lru_tail = e;
    c->lru_head = e;
}

void cache_destroy(LRUCache *c) {
    if (!c) return;
    CacheEntry *e = c->lru_head;
    while (e) {
        CacheEntry *nxt = e->next;
        free_cache_rows(e->rows);
        free(e);
        e = nxt;
    }
    pthread_mutex_destroy(&c->lock);
    free(c);
}

CacheEntry *cache_get(LRUCache *c, const char *key) {
    pthread_mutex_lock(&c->lock);
    unsigned int b = cache_hash(key, c->capacity);
    CacheEntry *e = c->buckets[b];
    while (e) {
        if (strcmp(e->key, key) == 0) {
            /* Move to front */
            lru_remove(c, e);
            lru_push_front(c, e);
            pthread_mutex_unlock(&c->lock);
            return e;
        }
        e = e->hash_next;
    }
    pthread_mutex_unlock(&c->lock);
    return NULL;
}

void cache_put(LRUCache *c, const char *key, CacheRow *rows, int nrows) {
    pthread_mutex_lock(&c->lock);
    unsigned int b = cache_hash(key, c->capacity);

    /* Check if already exists */
    CacheEntry *e = c->buckets[b];
    while (e) {
        if (strcmp(e->key, key) == 0) {
            free_cache_rows(e->rows);
            e->rows  = rows;
            e->nrows = nrows;
            lru_remove(c, e);
            lru_push_front(c, e);
            pthread_mutex_unlock(&c->lock);
            return;
        }
        e = e->hash_next;
    }

    /* Evict LRU if at capacity */
    if (c->size >= c->capacity && c->lru_tail) {
        CacheEntry *victim = c->lru_tail;
        lru_remove(c, victim);
        /* Remove from hash */
        unsigned int vb = cache_hash(victim->key, c->capacity);
        CacheEntry **pp = &c->buckets[vb];
        while (*pp && *pp != victim) pp = &(*pp)->hash_next;
        if (*pp) *pp = victim->hash_next;
        free_cache_rows(victim->rows);
        free(victim);
        c->size--;
    }

    CacheEntry *ne = calloc(1, sizeof(CacheEntry));
    strncpy(ne->key, key, CACHE_KEY_MAX-1);
    ne->rows  = rows;
    ne->nrows = nrows;
    ne->hash_next = c->buckets[b];
    c->buckets[b] = ne;
    lru_push_front(c, ne);
    c->size++;
    pthread_mutex_unlock(&c->lock);
}

void cache_invalidate(LRUCache *c, const char *table_name) {
    /* Remove all entries whose key contains the table name */
    pthread_mutex_lock(&c->lock);
    CacheEntry *e = c->lru_head;
    while (e) {
        CacheEntry *nxt = e->next;
        if (strcasestr(e->key, table_name)) {
            lru_remove(c, e);
            unsigned int b = cache_hash(e->key, c->capacity);
            CacheEntry **pp = &c->buckets[b];
            while (*pp && *pp != e) pp = &(*pp)->hash_next;
            if (*pp) *pp = e->hash_next;
            free_cache_rows(e->rows);
            free(e);
            c->size--;
        }
        e = nxt;
    }
    pthread_mutex_unlock(&c->lock);
}
