/*
 * query.c - Query execution engine.
 */
#include "../../include/query/query.h"
#include "../../include/storage/storage.h"
#include "../../include/index/index.h"
#include "../../include/cache/cache.h"
#include "../../include/parser/parser.h"
#include "../../include/flexql.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

QueryEngine *qe_create(Catalog *cat, IndexCatalog *ic, LRUCache *cache) {
    QueryEngine *qe=calloc(1,sizeof(QueryEngine));
    qe->catalog=cat; qe->idxcat=ic; qe->cache=cache; return qe;
}
void qe_destroy(QueryEngine *qe) { free(qe); }

/* ── Value helpers ─────────────────────────────────────────── */
static void value_to_str(const Value *v, char *buf, int bufsz) {
    if (v->is_null) { snprintf(buf,bufsz,"NULL"); return; }
    switch(v->type) {
        case TYPE_INT:
            snprintf(buf,bufsz,"%lld",(long long)v->v.ival); break;
        case TYPE_DECIMAL: {
            double dv=v->v.dval;
            if (dv==(long long)dv && dv>=-1e15 && dv<=1e15)
                snprintf(buf,bufsz,"%.0f",dv);
            else
                snprintf(buf,bufsz,"%g",dv);
            break;
        }
        case TYPE_VARCHAR:
            snprintf(buf,bufsz,"%s",v->v.sval?v->v.sval:""); break;
        default: snprintf(buf,bufsz,"?"); break;
    }
}

static int parse_value(const char *s, ColType type, Value *out) {
    out->is_null=0; out->type=type;
    if (strcasecmp(s,"NULL")==0) { out->is_null=1; return 0; }
    switch(type) {
        case TYPE_INT:     out->v.ival=atoll(s); break;
        case TYPE_DECIMAL: out->v.dval=atof(s);  break;
        case TYPE_VARCHAR: out->v.sval=strdup(s); break;
        default: return -1;
    }
    return 0;
}

static int compare_values(const Value *a, const Value *b) {
    if (a->is_null&&b->is_null) return 0;
    if (a->is_null) return -1;
    if (b->is_null) return  1;
    if (a->type==TYPE_VARCHAR||b->type==TYPE_VARCHAR) {
        char sa[512],sb[512];
        value_to_str(a,sa,sizeof(sa)); value_to_str(b,sb,sizeof(sb));
        return strcmp(sa,sb);
    }
    double da=(a->type==TYPE_INT)?(double)a->v.ival:a->v.dval;
    double db=(b->type==TYPE_INT)?(double)b->v.ival:b->v.dval;
    return (da<db)?-1:(da>db)?1:0;
}

static int eval_op(int cmp, CondOp op) {
    switch(op) {
        case OP_EQ:  return cmp==0;
        case OP_NEQ: return cmp!=0;
        case OP_LT:  return cmp< 0;
        case OP_GT:  return cmp> 0;
        case OP_LTE: return cmp<=0;
        case OP_GTE: return cmp>=0;
    }
    return 0;
}

static int find_col(const Schema *s, const char *name) {
    for (int i=0;i<s->ncols;i++) if (strcasecmp(s->cols[i].name,name)==0) return i;
    return -1;
}

static int eval_where(const Condition *cond, const Row *row, const Schema *schema) {
    int col=find_col(schema,cond->lhs_col); if(col<0) return 0;
    Value rhs; memset(&rhs,0,sizeof(rhs)); rhs.type=row->vals[col].type;
    parse_value(cond->rhs_literal,rhs.type,&rhs);
    int result=eval_op(compare_values(&row->vals[col],&rhs),cond->op);
    if(rhs.type==TYPE_VARCHAR&&!rhs.is_null&&rhs.v.sval) free(rhs.v.sval);
    return result;
}

static int row_alive(const Row *r) {
    return (r->expires_at==0)||(time(NULL)<r->expires_at);
}

static void free_str_arr(char **arr, int n) {
    for(int i=0;i<n;i++) free(arr[i]); free(arr);
}

static char **project_row(const Row *row, const int *proj, int n) {
    char **out=calloc(n,sizeof(char*));
    for(int i=0;i<n;i++) {
        char buf[512]; value_to_str(&row->vals[proj[i]],buf,sizeof(buf));
        out[i]=strdup(buf);
    }
    return out;
}

/*
 * Build projection for a schema given the SELECT column list.
 * all=1: emit all columns (used for JOIN tables and SELECT *)
 * Returns 0 on success, -1 if a requested column doesn't exist.
 *
 * For JOIN queries: each SelCol may have a table qualifier.
 * We match cols that either have no qualifier OR whose qualifier matches schema->name.
 */
static int build_projection(const ParsedStmt *stmt, const Schema *schema,
                             int all, int *proj, char **names, int *proj_n) {
    if (all || stmt->sel_star) {
        *proj_n=schema->ncols;
        for(int i=0;i<schema->ncols;i++) { proj[i]=i; names[i]=strdup(schema->cols[i].name); }
        return 0;
    }
    *proj_n=0;
    for(int si=0;si<stmt->sel_ncols;si++) {
        const SelCol *sc=&stmt->sel_cols[si];
        /* Skip columns qualified for a different table */
        if(sc->table[0]!='\0' && strcasecmp(sc->table,schema->name)!=0) continue;
        int ci=find_col(schema,sc->col);
        if(ci<0) {
            /* Unknown column only errors if no table qualifier and no join */
            /* With a join, the column might belong to the other table - skip */
            continue;
        }
        proj[*proj_n]=ci; names[*proj_n]=strdup(schema->cols[ci].name); (*proj_n)++;
    }
    return 0; /* 0 even if proj_n==0, caller handles */
}



/* ── exec_create ──────────────────────────────────────────── */
static int exec_create(QueryEngine *qe, ParsedStmt *stmt, char **errmsg) {
    char err[256];
    if(catalog_create_table(qe->catalog,&stmt->schema,err)!=0) {
        if(errmsg) *errmsg=strdup(err); return FLEXQL_ERROR;
    }
    if(stmt->schema.pk_col>=0) idxcat_get_or_create(qe->idxcat,stmt->schema.name);
    catalog_persist(qe->catalog,NULL); return FLEXQL_OK;
}

/* ── exec_insert (with batch support) ────────────────────── */
static int exec_insert(QueryEngine *qe, ParsedStmt *stmt, char **errmsg) {
    Table *t=catalog_find_table(qe->catalog,stmt->ins_table);
    if(!t) {
        if(errmsg) { char e[128]; snprintf(e,sizeof(e),"Table '%s' not found",stmt->ins_table); *errmsg=strdup(e); }
        return FLEXQL_ERROR;
    }
    if(stmt->ins_nvals!=t->schema.ncols) {
        if(errmsg) *errmsg=strdup("Column count mismatch"); return FLEXQL_ERROR;
    }

    /* Insert first row */
    Row row; row.ncols=t->schema.ncols; row.expires_at=(time_t)stmt->ins_expires; row.next=NULL;
    row.vals=calloc(row.ncols,sizeof(Value));
    for(int i=0;i<row.ncols;i++) {
        if(parse_value(stmt->ins_vals[i],t->schema.cols[i].type,&row.vals[i])!=0) {
            if(errmsg) *errmsg=strdup("Type parse error");
            free(row.vals); return FLEXQL_ERROR;
        }
    }
    table_insert_row(t,&row,NULL);
    if(t->schema.pk_col>=0) {
        HashIndex *idx=idxcat_get_or_create(qe->idxcat,t->schema.name);
        char pk_str[256]; value_to_str(&row.vals[t->schema.pk_col],pk_str,sizeof(pk_str));
        Page *pg=t->head; while(pg&&pg->next) pg=pg->next;
        if(pg&&pg->nrows>0) index_insert(idx,pk_str,&pg->rows[pg->nrows-1]);
    }
    {
        Page *pg=t->head; while(pg&&pg->next) pg=pg->next;
        if(pg&&pg->nrows>0) fast_append_row(qe->catalog,t->schema.name,&pg->rows[pg->nrows-1]);
    }
    for(int i=0;i<row.ncols;i++)
        if(row.vals[i].type==TYPE_VARCHAR&&!row.vals[i].is_null&&row.vals[i].v.sval)
            free(row.vals[i].v.sval);
    free(row.vals);

    /* Batch: parse additional (val1,val2,...) groups after first row */
    if (stmt->batch_rest) {
        const char *p = stmt->batch_rest;
        while (*p) {
            /* skip whitespace and commas */
            while (*p && (isspace((unsigned char)*p) || *p==',')) p++;
            if (*p!='(') break;
            p++; /* consume '(' */
            /* parse values */
            char bvals[MAX_INSERT_VALS][256];
            int bnvals=0;
            while (*p && *p!=')') {
                while (*p && isspace((unsigned char)*p)) p++;
                if (*p==',') { p++; continue; }
                /* Read one value */
                char vbuf[256]; int vi=0;
                if (*p=='\'') {
                    p++;
                    while (*p && *p!='\'') { if(vi<255){vbuf[vi]=*p;vi++;} p++; }
                    if(*p=='\'') p++;
                } else {
                    while (*p && *p!=',' && *p!=')') { if(vi<255){vbuf[vi]=*p;vi++;} p++; }
                }
                vbuf[vi]='\0';
                if(bnvals<MAX_INSERT_VALS) { strncpy(bvals[bnvals],vbuf,255); bnvals++; }
            }
            if(*p==')') p++;
            if(bnvals!=t->schema.ncols) continue; /* skip bad row */
            /* Insert batch row */
            Row brow; brow.ncols=t->schema.ncols; brow.expires_at=0; brow.next=NULL;
            brow.vals=calloc(brow.ncols,sizeof(Value));
            int ok=1;
            for(int i=0;i<brow.ncols;i++) {
                if(parse_value(bvals[i],t->schema.cols[i].type,&brow.vals[i])!=0) { ok=0; break; }
            }
            if(ok) {
                table_insert_row(t,&brow,NULL);
                if(t->schema.pk_col>=0) {
                    HashIndex *idx=idxcat_get_or_create(qe->idxcat,t->schema.name);
                    char pk_str[256]; value_to_str(&brow.vals[t->schema.pk_col],pk_str,sizeof(pk_str));
                    Page *pg=t->head; while(pg&&pg->next) pg=pg->next;
                    if(pg&&pg->nrows>0) index_insert(idx,pk_str,&pg->rows[pg->nrows-1]);
                }
                Page *pg=t->head; while(pg&&pg->next) pg=pg->next;
                if(pg&&pg->nrows>0) fast_append_row(qe->catalog,t->schema.name,&pg->rows[pg->nrows-1]);
            }
            for(int i=0;i<brow.ncols;i++)
                if(brow.vals[i].type==TYPE_VARCHAR&&!brow.vals[i].is_null&&brow.vals[i].v.sval)
                    free(brow.vals[i].v.sval);
            free(brow.vals);
        }
    }

    cache_invalidate(qe->cache,stmt->ins_table);
    return FLEXQL_OK;
}

/* ── exec_select ──────────────────────────────────────────── */
static int exec_select(QueryEngine *qe, ParsedStmt *stmt,
                        int (*cb)(void*,int,char**,char**), void *arg, char **errmsg) {
    Table *ta=catalog_find_table(qe->catalog,stmt->sel_from);
    if(!ta) {
        if(errmsg) { char e[128]; snprintf(e,sizeof(e),"Table '%s' not found",stmt->sel_from); *errmsg=strdup(e); }
        return FLEXQL_ERROR;
    }
    int is_join=(stmt->sel_join[0]!='\0');
    Table *tb=NULL;
    if(is_join) {
        tb=catalog_find_table(qe->catalog,stmt->sel_join);
        if(!tb) {
            if(errmsg) { char e[128]; snprintf(e,sizeof(e),"Table '%s' not found",stmt->sel_join); *errmsg=strdup(e); }
            return FLEXQL_ERROR;
        }
    }
    if(!cb) return FLEXQL_OK;

    /* Build projections */
    int proj_a[MAX_COLS]; char *names_a[MAX_COLS]; int proj_n_a=0;
    int proj_b[MAX_COLS]; char *names_b[MAX_COLS]; int proj_n_b=0;

    if (!is_join) {
        /* Non-join: validate that all requested columns exist */
        if (!stmt->sel_star && stmt->sel_ncols > 0) {
            for(int si=0;si<stmt->sel_ncols;si++) {
                /* Skip table-qualified columns for now (for forward compat) */
                int ci=find_col(&ta->schema,stmt->sel_cols[si].col);
                if(ci<0) {
                    if(errmsg) { char e[128]; snprintf(e,sizeof(e),"Unknown column '%s'",stmt->sel_cols[si].col); *errmsg=strdup(e); }
                    return FLEXQL_ERROR;
                }
            }
        }
        build_projection(stmt,&ta->schema,0,proj_a,names_a,&proj_n_a);
    } else {
        /* JOIN: build per-table projections */
        if(stmt->sel_star) {
            /* SELECT * → all cols from both tables */
            build_projection(stmt,&ta->schema,1,proj_a,names_a,&proj_n_a);
            build_projection(stmt,&tb->schema,1,proj_b,names_b,&proj_n_b);
        } else {
            /* Specific columns: split by table qualifier */
            build_projection(stmt,&ta->schema,0,proj_a,names_a,&proj_n_a);
            build_projection(stmt,&tb->schema,0,proj_b,names_b,&proj_n_b);
            /* If absolutely nothing matched (no qualifiers at all), fall back to all */
            if(proj_n_a==0 && proj_n_b==0) {
                build_projection(stmt,&ta->schema,1,proj_a,names_a,&proj_n_a);
                build_projection(stmt,&tb->schema,1,proj_b,names_b,&proj_n_b);
            }
            /* If one side is empty but the other has matches, that's valid */
            /* e.g. SELECT ta.col FROM ta JOIN tb ON ... — only ta columns shown */
        }
    }

    /* Guard */
    int total_proj=proj_n_a+proj_n_b;
    if(total_proj<=0) { total_proj=1; }
    char **all_names=calloc((size_t)total_proj,sizeof(char*));
    for(int i=0;i<proj_n_a;i++) all_names[i]=names_a[i];
    for(int i=0;i<proj_n_b;i++) all_names[proj_n_a+i]=names_b[i];

    pthread_rwlock_rdlock(&ta->lock);
    if(is_join) pthread_rwlock_rdlock(&tb->lock);

    if(!is_join) {
        /* PK index fast path */
        int pk=ta->schema.pk_col;
        if(stmt->has_where && pk>=0 &&
           strcasecmp(stmt->where_cond.lhs_col,ta->schema.cols[pk].name)==0 &&
           stmt->where_cond.op==OP_EQ) {
            HashIndex *idx=idxcat_get(qe->idxcat,ta->schema.name);
            if(idx) {
                Row *r=index_lookup(idx,stmt->where_cond.rhs_literal);
                if(r&&row_alive(r)) {
                    char **vals=project_row(r,proj_a,proj_n_a);
                    cb(arg,proj_n_a,vals,all_names);
                    free_str_arr(vals,proj_n_a);
                }
                goto done_simple;
            }
        }
        for(Page *pg=ta->head;pg;pg=pg->next) {
            for(int ri=0;ri<pg->nrows;ri++) {
                Row *r=&pg->rows[ri];
                if(!row_alive(r)) continue;
                if(stmt->has_where&&!eval_where(&stmt->where_cond,r,&ta->schema)) continue;
                char **vals=project_row(r,proj_a,proj_n_a);
                int abort=cb(arg,proj_n_a,vals,all_names);
                free_str_arr(vals,proj_n_a);
                if(abort) goto done_simple;
            }
        }
        done_simple:;
    } else {
        Condition *jc=&stmt->join_cond;
        int jcol_a=find_col(&ta->schema,jc->lhs_col);
        int jcol_b=find_col(&tb->schema,jc->rhs_col);
        if(jcol_a<0||jcol_b<0) { jcol_a=find_col(&ta->schema,jc->rhs_col); jcol_b=find_col(&tb->schema,jc->lhs_col); }
        char **combined=calloc((size_t)(proj_n_a+proj_n_b),sizeof(char*));
        for(Page *pga=ta->head;pga;pga=pga->next) {
          for(int ra=0;ra<pga->nrows;ra++) {
            Row *rowa=&pga->rows[ra]; if(!row_alive(rowa)) continue;
            if(stmt->has_where) {
                const char *wt=stmt->where_cond.lhs_table;
                if(wt[0]=='\0'||strcasecmp(wt,ta->schema.name)==0)
                    if(!eval_where(&stmt->where_cond,rowa,&ta->schema)) continue;
            }
            for(Page *pgb=tb->head;pgb;pgb=pgb->next) {
              for(int rb=0;rb<pgb->nrows;rb++) {
                Row *rowb=&pgb->rows[rb]; if(!row_alive(rowb)) continue;
                if(jcol_a>=0&&jcol_b>=0)
                    if(!eval_op(compare_values(&rowa->vals[jcol_a],&rowb->vals[jcol_b]),jc->op)) continue;
                if(stmt->has_where) {
                    const char *wt=stmt->where_cond.lhs_table;
                    if(strcasecmp(wt,tb->schema.name)==0)
                        if(!eval_where(&stmt->where_cond,rowb,&tb->schema)) continue;
                }
                char **va=project_row(rowa,proj_a,proj_n_a);
                char **vb=project_row(rowb,proj_b,proj_n_b);
                for(int i=0;i<proj_n_a;i++) combined[i]=va[i];
                for(int i=0;i<proj_n_b;i++) combined[proj_n_a+i]=vb[i];
                free(va); free(vb);
                int abort=cb(arg,proj_n_a+proj_n_b,combined,all_names);
                for(int i=0;i<proj_n_a+proj_n_b;i++) { free(combined[i]); combined[i]=NULL; }
                if(abort) goto done_join;
              }
            }
          }
        }
        done_join:
        free(combined);
    }

    if(is_join) pthread_rwlock_unlock(&tb->lock);
    pthread_rwlock_unlock(&ta->lock);
    free_str_arr(all_names,proj_n_a+proj_n_b);
    return FLEXQL_OK;
}

/* ── Main dispatch ────────────────────────────────────────── */
int qe_execute(QueryEngine *qe, const char *raw_sql, ParsedStmt *stmt,
               int (*cb)(void*,int,char**,char**), void *arg, char **errmsg) {
    (void)raw_sql;
    switch(stmt->type) {
        case STMT_DROP_TABLE: {
            catalog_drop_table(qe->catalog,stmt->drop_table,NULL);
            if(stmt->drop_table[0]) cache_invalidate(qe->cache,stmt->drop_table);
            return FLEXQL_OK;
        }
        case STMT_CREATE_TABLE: return exec_create(qe,stmt,errmsg);
        case STMT_INSERT:       return exec_insert(qe,stmt,errmsg);
        case STMT_SELECT:       return exec_select(qe,stmt,cb,arg,errmsg);
        default:
            if(errmsg) *errmsg=strdup("Unsupported statement");
            return FLEXQL_ERROR;
    }
}
