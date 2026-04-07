#ifndef FLEXQL_TYPES_H
#define FLEXQL_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>

/* ── Column types ─────────────────────────────────────────── */
typedef enum {
    TYPE_INT,
    TYPE_DECIMAL,
    TYPE_VARCHAR,
    TYPE_UNKNOWN
} ColType;

/* ── Schema ───────────────────────────────────────────────── */
#define MAX_COL_NAME  64
#define MAX_COLS      64
#define MAX_TABLE_NAME 64

typedef struct {
    char    name[MAX_COL_NAME];
    ColType type;
    int     primary_key;   /* 1 if this is the primary key column */
    int     not_null;
    int     varchar_len;   /* max length for VARCHAR (0 = unlimited) */
} ColDef;

typedef struct {
    char   name[MAX_TABLE_NAME];
    int    ncols;
    ColDef cols[MAX_COLS];
    int    pk_col;         /* index of primary-key column (-1 if none) */
} Schema;

/* ── A single stored value ────────────────────────────────── */
typedef struct {
    ColType type;
    union {
        long long  ival;
        double     dval;
        char      *sval;   /* heap-allocated */
    } v;
    int is_null;
} Value;

/* ── A single row (array of Values) ──────────────────────── */
typedef struct Row {
    int      ncols;
    Value   *vals;          /* ncols elements */
    time_t   expires_at;    /* 0 = never expires */
    struct Row *next;       /* linked-list inside page */
} Row;

#endif /* FLEXQL_TYPES_H */
