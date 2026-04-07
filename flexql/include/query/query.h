#ifndef FLEXQL_QUERY_H
#define FLEXQL_QUERY_H

#include "../parser/parser.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../cache/cache.h"

typedef struct {
    Catalog      *catalog;
    IndexCatalog *idxcat;
    LRUCache     *cache;
} QueryEngine;

QueryEngine *qe_create(Catalog *cat, IndexCatalog *ic, LRUCache *cache);
void         qe_destroy(QueryEngine *qe);

/*
 * Execute a parsed statement.
 * For SELECT, callback is invoked once per result row.
 * Returns FLEXQL_OK or FLEXQL_ERROR; on error, *errmsg is malloc'd.
 */
int qe_execute(
    QueryEngine  *qe,
    const char   *raw_sql,
    ParsedStmt   *stmt,
    int         (*callback)(void*, int, char**, char**),
    void         *arg,
    char        **errmsg
);

#endif /* FLEXQL_QUERY_H */
