#ifndef FLEXQL_INDEX_H
#define FLEXQL_INDEX_H

#include "../common/types.h"

/*
 * Primary key index using a Left-Leaning Red-Black BST (equivalent to B-tree).
 *
 * Benefits over hash map:
 *   - O(log N) equality lookups (vs O(1) avg hash, but no worst-case guarantee)
 *   - O(log N) range query start (supports >, <, >=, <= via ordered traversal)
 *   - No hash collisions, no rehashing
 *   - Ordered iteration for future ORDER BY support
 */

typedef struct HashIndex HashIndex;   /* opaque - internally a RB-tree */

HashIndex *index_create(void);
void       index_destroy(HashIndex *idx);
void       index_insert(HashIndex *idx, const char *key, Row *row);
Row       *index_lookup(HashIndex *idx, const char *key);
void       index_delete(HashIndex *idx, const char *key);

/* In-order traversal callback for range scans */
typedef int (*IndexScanCb)(const char *key, Row *row, void *arg);
void       index_scan(HashIndex *idx, IndexScanCb cb, void *arg);

/* One index per table */
typedef struct TableIndex {
    char             table_name[64];
    HashIndex       *idx;
    struct TableIndex *next;
} TableIndex;

typedef struct {
    TableIndex *head;
} IndexCatalog;

IndexCatalog *idxcat_create(void);
void          idxcat_destroy(IndexCatalog *ic);
HashIndex    *idxcat_get(IndexCatalog *ic, const char *table);
HashIndex    *idxcat_get_or_create(IndexCatalog *ic, const char *table);

#endif /* FLEXQL_INDEX_H */
