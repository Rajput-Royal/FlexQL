/*
 * parser.c - Recursive-descent SQL parser for FlexQL.
 *
 * Key tokeniser rules:
 *  - Hard delimiters: ( ) , ; = < >  — always their own token
 *  - Two-char operators: >= <= !=
 *  - Dot '.': qualifier separator ONLY when buf is a pure identifier AND next is alpha/_
 *             Inside decimals (95.5) and emails (a@b.com) the dot stays in the token
 *  - WHERE RHS value: everything up to ';' or end-of-string (handles spaces in values)
 *  - String literals 'text': quotes stripped, content returned as-is
 */
#include "../../include/parser/parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_TOK 512
typedef struct { const char *p; char tok[MAX_TOK]; int eof; } Lexer;

static void lex_init(Lexer *l, const char *sql) { l->p=sql; l->eof=0; l->tok[0]='\0'; }
static void skip_ws(Lexer *l) { while(*l->p && isspace((unsigned char)*l->p)) l->p++; }

static int is_hard_delim(char c) {
    return c=='('||c==')'||c==','||c==';'||c=='='||c=='<'||c=='>';
}

/* Dot is a qualifier separator when:
 * - next char after dot is letter/underscore
 * - buf so far is a pure SQL identifier (no '@' or '.') and non-empty */
static int token_ends_here(const char *p, const char *buf, int buf_len) {
    char c=*p;
    if (!c || isspace((unsigned char)c)) return 1;
    if (is_hard_delim(c)) return 1;
    if (c=='.') {
        char nx=*(p+1);
        if (!isalpha((unsigned char)nx) && nx!='_') return 0; /* decimal: keep */
        if (buf_len==0) return 0; /* handled as standalone token pre-loop */
        for (int i=0;i<buf_len;i++) {
            char bc=buf[i];
            if (bc=='@'||bc=='.') return 0; /* email/chained: keep */
            if (!isalnum((unsigned char)bc)&&bc!='_') return 0;
        }
        return 1; /* pure ident.col: split */
    }
    return 0;
}

static int is_qualifier_dot(const char *p) {
    if (*p!='.') return 0;
    char nx=*(p+1);
    return isalpha((unsigned char)nx)||nx=='_';
}

static void lex_peek(Lexer *l, char *buf, int bufsz) {
    const char *save=l->p; skip_ws(l);
    if (!*l->p) { buf[0]='\0'; l->p=save; return; }
    int i=0;
    if (*l->p=='\'') {
        l->p++;
        while (*l->p && *l->p!='\'') { if(i<bufsz-1){buf[i]=*l->p;i++;} l->p++; }
        if(*l->p=='\'') l->p++;
        buf[i]='\0'; l->p=save; return;
    }
    if ((*l->p=='>'||*l->p=='<'||*l->p=='!')&&*(l->p+1)=='=') {
        buf[0]=l->p[0]; buf[1]=l->p[1]; buf[2]='\0'; l->p=save; return;
    }
    if (is_hard_delim(*l->p)) { buf[0]=*l->p; buf[1]='\0'; l->p=save; return; }
    if (is_qualifier_dot(l->p)) { buf[0]='.'; buf[1]='\0'; l->p=save; return; }
    while (!token_ends_here(l->p,buf,i)) { if(i<bufsz-1){buf[i]=*l->p;i++;} l->p++; }
    buf[i]='\0'; l->p=save;
}

static const char *lex_next(Lexer *l) {
    skip_ws(l);
    if (!*l->p) { l->tok[0]='\0'; l->eof=1; return l->tok; }
    int i=0;
    if (*l->p=='\'') {
        l->p++;
        while (*l->p && *l->p!='\'') { if(i<MAX_TOK-1){l->tok[i]=*l->p;i++;} l->p++; }
        if(*l->p=='\'') l->p++;
        l->tok[i]='\0'; return l->tok;
    }
    if ((*l->p=='>'||*l->p=='<'||*l->p=='!')&&*(l->p+1)=='=') {
        l->tok[0]=*l->p++; l->tok[1]=*l->p++; l->tok[2]='\0'; return l->tok;
    }
    if (is_hard_delim(*l->p)) { l->tok[0]=*l->p++; l->tok[1]='\0'; return l->tok; }
    if (is_qualifier_dot(l->p)) { l->tok[0]=*l->p++; l->tok[1]='\0'; return l->tok; }
    while (!token_ends_here(l->p,l->tok,i)) { if(i<MAX_TOK-1){l->tok[i]=*l->p;i++;} l->p++; }
    l->tok[i]='\0'; return l->tok;
}

/*
 * Read the WHERE/JOIN RHS value — everything up to ';', end-of-string,
 * or a keyword that starts a new clause (WHERE, ORDER, etc.)
 * This allows unquoted multi-word values like: WHERE NAME = hello world
 */
/* Read WHERE RHS value: stops at AND/OR/;/end.
 * Handles quoted strings and plain values with spaces.
 * e.g. "hello world" → "hello world"
 *      "1 AND x > 2" → "1"  (stops before AND) */
static void lex_rhs_value(Lexer *l, char *buf, int bufsz) {
    skip_ws(l);
    /* Quoted string literal */
    if (*l->p=='\'') {
        l->p++;
        int i=0;
        while (*l->p && *l->p!='\'') { if(i<bufsz-1){buf[i]=*l->p;i++;} l->p++; }
        if(*l->p=='\'') l->p++;
        buf[i]='\0'; return;
    }
    /* Unquoted: accumulate tokens, stop at SQL boundaries */
    int i=0;
    const char *save_start = l->p;
    while (*l->p && *l->p!=';') {
        /* Check if next word-boundary token is AND/OR */
        if (isspace((unsigned char)*l->p)) {
            /* peek ahead past whitespace for keyword */
            const char *tmp = l->p;
            while (*tmp && isspace((unsigned char)*tmp)) tmp++;
            /* Check for AND/OR keyword boundary */
            if (strncasecmp(tmp,"AND",3)==0 &&
                (tmp[3]=='\0'||isspace((unsigned char)tmp[3])||tmp[3]==';'))
                break;
            if (strncasecmp(tmp,"OR",2)==0 &&
                (tmp[2]=='\0'||isspace((unsigned char)tmp[2])||tmp[2]==';'))
                break;
        }
        if (i<bufsz-1) { buf[i]=*l->p; i++; }
        l->p++;
    }
    (void)save_start;
    /* trim trailing whitespace */
    while (i>0 && isspace((unsigned char)buf[i-1])) i--;
    buf[i]='\0';
}

static int tok_is(const char *a, const char *b) { return strcasecmp(a,b)==0; }
static void to_upper(char *s) { for(;*s;s++) *s=(char)toupper((unsigned char)*s); }
static void scopy(char *dst, const char *src, size_t dstsz) {
    size_t i=0; for(;i+1<dstsz&&src[i];i++) dst[i]=src[i]; dst[i]='\0';
}

static int parse_op(const char *t, CondOp *op) {
    if(!strcmp(t,"="))           { *op=OP_EQ;  return 0; }
    if(!strcmp(t,"!=")||!strcmp(t,"<>")) { *op=OP_NEQ; return 0; }
    if(!strcmp(t,"<"))           { *op=OP_LT;  return 0; }
    if(!strcmp(t,">"))           { *op=OP_GT;  return 0; }
    if(!strcmp(t,"<="))          { *op=OP_LTE; return 0; }
    if(!strcmp(t,">="))          { *op=OP_GTE; return 0; }
    return -1;
}

static ColType parse_coltype(const char *s) {
    if(tok_is(s,"INT")||tok_is(s,"INTEGER")||tok_is(s,"BIGINT"))                      return TYPE_INT;
    if(tok_is(s,"DECIMAL")||tok_is(s,"FLOAT")||tok_is(s,"DOUBLE")||tok_is(s,"REAL")) return TYPE_DECIMAL;
    if(tok_is(s,"TEXT")||tok_is(s,"VARCHAR")||tok_is(s,"CHAR")||tok_is(s,"STRING"))  return TYPE_VARCHAR;
    return TYPE_UNKNOWN;
}

static void parse_qualified(Lexer *l, char *tbl, char *col) {
    char peek[MAX_TOK]; tbl[0]='\0';
    lex_next(l); scopy(col,l->tok,MAX_COL_NAME);
    lex_peek(l,peek,sizeof(peek));
    if(!strcmp(peek,".")) {
        lex_next(l); scopy(tbl,col,MAX_TABLE_NAME);
        lex_next(l); scopy(col,l->tok,MAX_COL_NAME);
    }
}

/* ── CREATE TABLE ─────────────────────────────────────────── */
static int parse_create(Lexer *l, ParsedStmt *out, char *err, int esz) {
    char peek[MAX_TOK];
    lex_next(l);
    if(!tok_is(l->tok,"TABLE")) { snprintf(err,esz,"Expected TABLE"); return -1; }
    lex_next(l); scopy(out->schema.name,l->tok,MAX_TABLE_NAME); to_upper(out->schema.name);
    lex_next(l);
    if(strcmp(l->tok,"(")!=0) { snprintf(err,esz,"Expected '('"); return -1; }
    out->schema.ncols=0; out->schema.pk_col=-1;
    for(;;) {
        lex_peek(l,peek,sizeof(peek));
        if(!strcmp(peek,")")||!strcmp(peek,";")||peek[0]=='\0') { lex_next(l); break; }
        if(!strcmp(peek,",")) { lex_next(l); continue; }
        if(out->schema.ncols>=MAX_COLS) { snprintf(err,esz,"Too many columns"); return -1; }
        ColDef *cd=&out->schema.cols[out->schema.ncols];
        memset(cd,0,sizeof(*cd));
        lex_next(l); scopy(cd->name,l->tok,MAX_COL_NAME); to_upper(cd->name);
        lex_next(l); cd->type=parse_coltype(l->tok);
        if(cd->type==TYPE_UNKNOWN) { snprintf(err,esz,"Unknown type '%s'",l->tok); return -1; }
        lex_peek(l,peek,sizeof(peek));
        if(!strcmp(peek,"(")) { lex_next(l); lex_next(l); cd->varchar_len=atoi(l->tok); lex_next(l); }
        for(;;) {
            lex_peek(l,peek,sizeof(peek));
            if(tok_is(peek,"PRIMARY"))   { lex_next(l); lex_next(l); cd->primary_key=1; out->schema.pk_col=out->schema.ncols; }
            else if(tok_is(peek,"NOT"))  { lex_next(l); lex_next(l); cd->not_null=1; }
            else if(tok_is(peek,"NULL")) { lex_next(l); }
            else break;
        }
        out->schema.ncols++;
    }
    return 0;
}

/* ── INSERT (supports multi-row batch: VALUES (r1),(r2),...) ─ */
static int parse_insert(Lexer *l, ParsedStmt *out, char *err, int esz) {
    char peek[MAX_TOK];
    lex_next(l);
    if(!tok_is(l->tok,"INTO")) { snprintf(err,esz,"Expected INTO"); return -1; }
    lex_next(l); scopy(out->ins_table,l->tok,MAX_TABLE_NAME); to_upper(out->ins_table);
    /* optional column list */
    lex_peek(l,peek,sizeof(peek));
    if(!strcmp(peek,"(")) {
        const char *saved=l->p; lex_next(l);
        lex_peek(l,peek,sizeof(peek));
        if(!tok_is(peek,"VALUES")) {
            int depth=1;
            while(depth>0&&!l->eof) { lex_next(l); if(!strcmp(l->tok,"("))depth++; if(!strcmp(l->tok,")"))depth--; }
        } else { l->p=saved; }
    }
    lex_next(l);
    if(!tok_is(l->tok,"VALUES")) { snprintf(err,esz,"Expected VALUES"); return -1; }
    /* Read first row — we store only the first for single-insert path */
    lex_next(l);
    if(strcmp(l->tok,"(")!=0) { snprintf(err,esz,"Expected '(' after VALUES"); return -1; }
    out->ins_nvals=0; out->ins_expires=0;
    for(;;) {
        lex_peek(l,peek,sizeof(peek));
        if(!strcmp(peek,")")||peek[0]=='\0') { lex_next(l); break; }
        if(!strcmp(peek,",")) { lex_next(l); continue; }
        lex_next(l);
        if(out->ins_nvals<MAX_INSERT_VALS) {
            scopy(out->ins_vals[out->ins_nvals],l->tok,sizeof(out->ins_vals[0]));
            out->ins_nvals++;
        }
    }
    /* Store pointer to rest of SQL for multi-row batch parsing */
    out->batch_rest = l->p;
    return 0;
}

/* ── SELECT ───────────────────────────────────────────────── */
static int parse_select(Lexer *l, ParsedStmt *out, char *err, int esz) {
    char peek[MAX_TOK];
    out->sel_ncols=0; out->sel_star=0; out->has_where=0; out->sel_join[0]='\0';
    lex_peek(l,peek,sizeof(peek));
    if(!strcmp(peek,"*")) { lex_next(l); out->sel_star=1; }
    else {
        while(1) {
            lex_peek(l,peek,sizeof(peek));
            if(tok_is(peek,"FROM")||peek[0]=='\0') break;
            if(!strcmp(peek,",")) { lex_next(l); continue; }
            if(out->sel_ncols>=MAX_SEL_COLS) break;
            SelCol *sc=&out->sel_cols[out->sel_ncols];
            sc->star=0; sc->table[0]='\0';
            parse_qualified(l,sc->table,sc->col);
            to_upper(sc->table); to_upper(sc->col);
            out->sel_ncols++;
        }
    }
    lex_next(l);
    if(!tok_is(l->tok,"FROM")) { snprintf(err,esz,"Expected FROM"); return -1; }
    lex_next(l); scopy(out->sel_from,l->tok,MAX_TABLE_NAME); to_upper(out->sel_from);
    lex_peek(l,peek,sizeof(peek));
    if(tok_is(peek,"INNER")||tok_is(peek,"JOIN")) {
        if(tok_is(peek,"INNER")) lex_next(l);
        lex_next(l);
        lex_next(l); scopy(out->sel_join,l->tok,MAX_TABLE_NAME); to_upper(out->sel_join);
        lex_next(l); /* ON */
        Condition *jc=&out->join_cond; memset(jc,0,sizeof(*jc));
        parse_qualified(l,jc->lhs_table,jc->lhs_col);
        to_upper(jc->lhs_table); to_upper(jc->lhs_col);
        lex_next(l);
        if(parse_op(l->tok,&jc->op)!=0) { snprintf(err,esz,"Bad JOIN op '%s'",l->tok); return -1; }
        parse_qualified(l,jc->rhs_table,jc->rhs_col);
        to_upper(jc->rhs_table); to_upper(jc->rhs_col);
        jc->is_join_cond=1; jc->rhs_literal[0]='\0';
    }
    lex_peek(l,peek,sizeof(peek));
    if(tok_is(peek,"WHERE")) {
        lex_next(l); out->has_where=1;
        Condition *wc=&out->where_cond; memset(wc,0,sizeof(*wc));
        parse_qualified(l,wc->lhs_table,wc->lhs_col);
        to_upper(wc->lhs_table); to_upper(wc->lhs_col);
        lex_next(l);
        if(parse_op(l->tok,&wc->op)!=0) { snprintf(err,esz,"Bad WHERE op '%s'",l->tok); return -1; }
        /* Use lex_rhs_value to capture multi-word values */
        lex_rhs_value(l, wc->rhs_literal, sizeof(wc->rhs_literal));
        wc->is_join_cond=0; wc->rhs_table[0]='\0'; wc->rhs_col[0]='\0';
        /* Reject AND/OR */
        lex_peek(l,peek,sizeof(peek));
        if(tok_is(peek,"AND")||tok_is(peek,"OR")) {
            snprintf(err,esz,"AND/OR not supported: only one WHERE condition allowed");
            return -1;
        }
    }
    return 0;
}

/* ── DROP TABLE ───────────────────────────────────────────── */
static int parse_drop(Lexer *l, ParsedStmt *out, char *err, int esz) {
    char peek[MAX_TOK];
    lex_next(l);
    if(!tok_is(l->tok,"TABLE")) { snprintf(err,esz,"Expected TABLE after DROP"); return -1; }
    lex_peek(l,peek,sizeof(peek));
    if(tok_is(peek,"IF")) { lex_next(l); lex_next(l); } /* IF EXISTS */
    lex_next(l); scopy(out->drop_table,l->tok,MAX_TABLE_NAME); to_upper(out->drop_table);
    return 0;
}

/* ── Entry point ──────────────────────────────────────────── */
int parse_sql(const char *sql, ParsedStmt *out, char *errbuf, int errbufsz) {
    Lexer l; lex_init(&l,sql);
    memset(out,0,sizeof(*out)); out->type=STMT_UNKNOWN; out->batch_rest=NULL;
    lex_next(&l);
    if(l.eof||l.tok[0]=='\0') { snprintf(errbuf,errbufsz,"Empty statement"); return -1; }
    if(tok_is(l.tok,"CREATE")) { out->type=STMT_CREATE_TABLE; return parse_create(&l,out,errbuf,errbufsz); }
    if(tok_is(l.tok,"INSERT")) { out->type=STMT_INSERT;       return parse_insert(&l,out,errbuf,errbufsz); }
    if(tok_is(l.tok,"SELECT")) { out->type=STMT_SELECT;       return parse_select(&l,out,errbuf,errbufsz); }
    if(tok_is(l.tok,"DROP"))   { out->type=STMT_DROP_TABLE;   return parse_drop(&l,out,errbuf,errbufsz); }
    snprintf(errbuf,errbufsz,"Unsupported statement: '%s'",l.tok);
    return -1;
}
