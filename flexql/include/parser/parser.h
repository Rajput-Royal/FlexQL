#ifndef FLEXQL_PARSER_H
#define FLEXQL_PARSER_H

#include "../common/types.h"

/* ── Statement types ──────────────────────────────────────── */
typedef enum {
    STMT_CREATE_TABLE,
    STMT_DROP_TABLE,
    STMT_INSERT,
    STMT_SELECT,
    STMT_UNKNOWN
} StmtType;

/* ── Condition operators ──────────────────────────────────── */
typedef enum {
    OP_EQ, OP_NEQ, OP_LT, OP_GT, OP_LTE, OP_GTE
} CondOp;

/* ── A single WHERE / JOIN condition ─────────────────────── */
typedef struct {
    char    lhs_table[MAX_TABLE_NAME];   /* table qualifier (may be empty) */
    char    lhs_col[MAX_COL_NAME];
    CondOp  op;
    char    rhs_table[MAX_TABLE_NAME];   /* non-empty for JOIN ON conditions */
    char    rhs_col[MAX_COL_NAME];
    char    rhs_literal[256];            /* non-empty for WHERE literal conds */
    int     is_join_cond;                /* 1 = JOIN ON, 0 = WHERE literal   */
} Condition;

/* ── SELECT column spec ───────────────────────────────────── */
typedef struct {
    char table[MAX_TABLE_NAME];          /* may be empty / "*" */
    char col[MAX_COL_NAME];
    int  star;                           /* 1 = SELECT * */
} SelCol;

#define MAX_SEL_COLS 64
#define MAX_INSERT_VALS 64

/* ── Parsed statement ─────────────────────────────────────── */
typedef struct {
    StmtType type;

    /* CREATE TABLE */
    Schema schema;

    /* DROP TABLE */
    char   drop_table[MAX_TABLE_NAME];

    /* INSERT */
    char   ins_table[MAX_TABLE_NAME];
    char   ins_vals[MAX_INSERT_VALS][256];  /* flat: row0_col0, row0_col1, ..., row1_col0... */
    int    ins_nvals;       /* values per row (= schema ncols) */
    int    ins_batch_rows;  /* number of rows in this INSERT (>=1) */
    long long ins_expires;   /* 0 = no expiry; epoch seconds */
    const char *batch_rest;    /* points to rest of SQL after first VALUES row */

    /* SELECT */
    char   sel_from[MAX_TABLE_NAME];
    char   sel_join[MAX_TABLE_NAME];     /* empty = no JOIN */
    Condition join_cond;
    SelCol sel_cols[MAX_SEL_COLS];
    int    sel_ncols;
    int    sel_star;
    int    has_where;
    Condition where_cond;
} ParsedStmt;

/* ── API ──────────────────────────────────────────────────── */
int parse_sql(const char *sql, ParsedStmt *out, char *errbuf, int errbufsz);

#endif /* FLEXQL_PARSER_H */
