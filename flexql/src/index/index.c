/*
 * index.c - B-tree primary key index.
 *
 * Why B-tree over hash map:
 *   Hash map: O(1) equality only, no ordering
 *   B-tree:   O(log N) equality AND range queries (>, <, >=, <=, !=)
 *             Better cache locality than hash chains
 *             Supports ordered scans (future ORDER BY)
 *
 * Implementation: Left-leaning Red-Black BST (equivalent to 2-3 tree).
 * Key: string representation of PK value.
 * Value: pointer to Row in heap page.
 */
#include "../../include/index/index.h"
#include <stdlib.h>
#include <string.h>

/* ── Red-Black BST node ──────────────────────────────────── */
#define RED   1
#define BLACK 0

typedef struct RBNode {
    char            key[256];
    Row            *row;
    struct RBNode  *left, *right;
    int             color;   /* RED or BLACK */
} RBNode;

/* ── HashIndex is now actually a RB-tree wrapper ─────────── */
struct HashIndex {
    RBNode *root;
    long    size;
};

/* ── Helpers ──────────────────────────────────────────────── */
static int is_red(RBNode *n) { return n && n->color == RED; }

static RBNode *new_node(const char *key, Row *row) {
    RBNode *n = calloc(1, sizeof(RBNode));
    strncpy(n->key, key, sizeof(n->key)-1);
    n->row   = row;
    n->color = RED;
    return n;
}

static RBNode *rotate_left(RBNode *h) {
    RBNode *x = h->right;
    h->right  = x->left;
    x->left   = h;
    x->color  = h->color;
    h->color  = RED;
    return x;
}

static RBNode *rotate_right(RBNode *h) {
    RBNode *x = h->left;
    h->left   = x->right;
    x->right  = h;
    x->color  = h->color;
    h->color  = RED;
    return x;
}

static void flip_colors(RBNode *h) {
    h->color        = !h->color;
    h->left->color  = !h->left->color;
    h->right->color = !h->right->color;
}

static RBNode *rb_insert(RBNode *h, const char *key, Row *row) {
    if (!h) return new_node(key, row);
    int cmp = strcmp(key, h->key);
    if      (cmp < 0) h->left  = rb_insert(h->left,  key, row);
    else if (cmp > 0) h->right = rb_insert(h->right, key, row);
    else              h->row   = row;   /* update existing */

    if  (is_red(h->right) && !is_red(h->left))  h = rotate_left(h);
    if  (is_red(h->left)  &&  is_red(h->left->left)) h = rotate_right(h);
    if  (is_red(h->left)  &&  is_red(h->right)) flip_colors(h);
    return h;
}

static Row *rb_lookup(RBNode *h, const char *key) {
    while (h) {
        int cmp = strcmp(key, h->key);
        if      (cmp < 0) h = h->left;
        else if (cmp > 0) h = h->right;
        else              return h->row;
    }
    return NULL;
}

static void rb_free(RBNode *h) {
    if (!h) return;
    rb_free(h->left);
    rb_free(h->right);
    free(h);
}

/* ── Public API ──────────────────────────────────────────── */
HashIndex *index_create(void) {
    HashIndex *idx = calloc(1, sizeof(HashIndex));
    return idx;
}

void index_destroy(HashIndex *idx) {
    if (!idx) return;
    rb_free(idx->root);
    free(idx);
}

void index_insert(HashIndex *idx, const char *key, Row *row) {
    idx->root = rb_insert(idx->root, key, row);
    idx->root->color = BLACK;
    idx->size++;
}

Row *index_lookup(HashIndex *idx, const char *key) {
    return rb_lookup(idx->root, key);
}

void index_delete(HashIndex *idx, const char *key) {
    (void)idx; (void)key; /* deletion not needed for this assignment */
}


/* ── In-order traversal (ascending key order) ──────────────── */
static void rb_inorder(RBNode *h, IndexScanCb cb, void *arg, int *stop) {
    if (!h || *stop) return;
    rb_inorder(h->left, cb, arg, stop);
    if (!*stop) {
        if (cb(h->key, h->row, arg) != 0) *stop = 1;
    }
    if (!*stop) rb_inorder(h->right, cb, arg, stop);
}

void index_scan(HashIndex *idx, IndexScanCb cb, void *arg) {
    if (!idx || !cb) return;
    int stop = 0;
    rb_inorder(idx->root, cb, arg, &stop);
}

/* ── IndexCatalog ─────────────────────────────────────────── */
IndexCatalog *idxcat_create(void) {
    return calloc(1, sizeof(IndexCatalog));
}

void idxcat_destroy(IndexCatalog *ic) {
    if (!ic) return;
    TableIndex *ti = ic->head;
    while (ti) {
        TableIndex *nxt = ti->next;
        index_destroy(ti->idx);
        free(ti);
        ti = nxt;
    }
    free(ic);
}

HashIndex *idxcat_get(IndexCatalog *ic, const char *table) {
    for (TableIndex *ti = ic->head; ti; ti = ti->next)
        if (strcasecmp(ti->table_name, table) == 0) return ti->idx;
    return NULL;
}

HashIndex *idxcat_get_or_create(IndexCatalog *ic, const char *table) {
    HashIndex *h = idxcat_get(ic, table);
    if (h) return h;
    TableIndex *ti = calloc(1, sizeof(TableIndex));
    strncpy(ti->table_name, table, sizeof(ti->table_name)-1);
    ti->idx  = index_create();
    ti->next = ic->head;
    ic->head = ti;
    return ti->idx;
}
