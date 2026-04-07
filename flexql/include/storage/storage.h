#ifndef FLEXQL_STORAGE_H
#define FLEXQL_STORAGE_H

#include "../common/types.h"
#include <pthread.h>
#include <stdio.h>

/* ── Page ─────────────────────────────────────────────────── */
#define PAGE_ROWS 256   /* rows per heap page */

typedef struct Page {
    Row         rows[PAGE_ROWS];
    int         nrows;
    struct Page *next;
} Page;

/* ── Table ────────────────────────────────────────────────── */
typedef struct Table {
    Schema       schema;
    Page        *head;          /* linked list of pages     */
    long         total_rows;
    FILE        *wal_fp;        /* kept-open WAL append handle */
    pthread_rwlock_t lock;
    struct Table *next;
} Table;

/* ── Catalog (collection of tables) ─────────────────────── */
typedef struct {
    Table          *tables;
    pthread_mutex_t mutex;
    char            data_dir[512];
} Catalog;

/* ── API ──────────────────────────────────────────────────── */
int fast_append_row(Catalog *cat, const char *table_name, const Row *row);
Catalog *catalog_create(const char *data_dir);
void     catalog_destroy(Catalog *cat);

Table   *catalog_find_table(Catalog *cat, const char *name);
int      catalog_create_table(Catalog *cat, const Schema *schema, char *err);

int      table_insert_row(Table *t, Row *row, char *err);
int      catalog_drop_table(Catalog *cat, const char *name, char *err);
/* Persist catalog+data to disk */
int      catalog_persist(Catalog *cat, char *err);
/* Load catalog from disk */
int      catalog_load(Catalog *cat, char *err);

#endif /* FLEXQL_STORAGE_H */
