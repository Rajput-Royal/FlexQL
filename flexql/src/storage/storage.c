/*
 * storage.c - Row-major heap storage with per-table read-write locks.
 *
 * Persistence strategy:
 *   - Each table has a .fql (full snapshot) and a .wal (append log).
 *   - INSERT appends one row to the WAL using a KEPT-OPEN file handle
 *     (no open/close per row = no disk sync overhead = fast inserts).
 *   - On shutdown, catalog_persist() rewrites the full .fql and removes the WAL.
 *   - On startup, both .fql and .wal are replayed into memory.
 */
#include "../../include/storage/storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <stdint.h>

static void ensure_dir(const char *path) { mkdir(path, 0755); }

static void value_free_inner(Value *v) {
    if (v->type==TYPE_VARCHAR && !v->is_null && v->v.sval) free(v->v.sval);
}
static void row_free_vals(Row *r) {
    for (int i=0;i<r->ncols;i++) value_free_inner(&r->vals[i]);
    free(r->vals);
}

/* ── Catalog lifecycle ───────────────────────────────────── */
Catalog *catalog_create(const char *data_dir) {
    Catalog *cat=calloc(1,sizeof(Catalog));
    pthread_mutex_init(&cat->mutex,NULL);
    snprintf(cat->data_dir,sizeof(cat->data_dir),"%s",data_dir);
    ensure_dir(data_dir);
    char tdir[600]; snprintf(tdir,sizeof(tdir),"%s/tables",data_dir);
    ensure_dir(tdir);
    return cat;
}

void catalog_destroy(Catalog *cat) {
    if (!cat) return;
    Table *t=cat->tables;
    while (t) {
        Table *nxt=t->next;
        /* Close WAL handle if open */
        if (t->wal_fp) { fflush(t->wal_fp); fclose(t->wal_fp); t->wal_fp=NULL; }
        Page *pg=t->head;
        while (pg) {
            Page *pnxt=pg->next;
            for (int i=0;i<pg->nrows;i++) if (pg->rows[i].vals) row_free_vals(&pg->rows[i]);
            free(pg); pg=pnxt;
        }
        pthread_rwlock_destroy(&t->lock);
        free(t); t=nxt;
    }
    pthread_mutex_destroy(&cat->mutex);
    free(cat);
}

Table *catalog_find_table(Catalog *cat, const char *name) {
    for (Table *t=cat->tables;t;t=t->next)
        if (strcasecmp(t->schema.name,name)==0) return t;
    return NULL;
}

int catalog_create_table(Catalog *cat, const Schema *schema, char *err) {
    pthread_mutex_lock(&cat->mutex);
    if (catalog_find_table(cat,schema->name)) {
        pthread_mutex_unlock(&cat->mutex);
        if (err) snprintf(err,256,"Table '%s' already exists",schema->name);
        return -1;
    }
    Table *t=calloc(1,sizeof(Table));
    t->schema=*schema;
    t->wal_fp=NULL;
    pthread_rwlock_init(&t->lock,NULL);
    t->next=cat->tables; cat->tables=t;
    pthread_mutex_unlock(&cat->mutex);
    return 0;
}

int table_insert_row(Table *t, Row *row, char *err) {
    (void)err;
    pthread_rwlock_wrlock(&t->lock);
    Page *pg=t->head, *last=NULL;
    while (pg && pg->nrows>=PAGE_ROWS) { last=pg; pg=pg->next; }
    if (!pg) { pg=calloc(1,sizeof(Page)); if (last) last->next=pg; else t->head=pg; }
    Row *slot=&pg->rows[pg->nrows];
    slot->ncols=row->ncols; slot->expires_at=row->expires_at; slot->next=NULL;
    slot->vals=calloc(row->ncols,sizeof(Value));
    for (int i=0;i<row->ncols;i++) {
        slot->vals[i]=row->vals[i];
        if (row->vals[i].type==TYPE_VARCHAR && !row->vals[i].is_null && row->vals[i].v.sval)
            slot->vals[i].v.sval=strdup(row->vals[i].v.sval);
    }
    pg->nrows++; t->total_rows++;
    pthread_rwlock_unlock(&t->lock);
    return 0;
}

/* ── Binary I/O helpers ──────────────────────────────────── */
static void write_u32(FILE *f, uint32_t v) { fwrite(&v,4,1,f); }
static void write_u64(FILE *f, uint64_t v) { fwrite(&v,8,1,f); }
static void write_str(FILE *f, const char *s) {
    uint32_t len=s?(uint32_t)strlen(s):0; write_u32(f,len);
    if (len) fwrite(s,1,len,f);
}
static int read_u32(FILE *f, uint32_t *v) { return fread(v,4,1,f)==1?0:-1; }
static int read_u64(FILE *f, uint64_t *v) { return fread(v,8,1,f)==1?0:-1; }
static char *read_str(FILE *f) {
    uint32_t len; if (read_u32(f,&len)!=0) return NULL;
    char *buf=calloc(len+1,1);
    if (len && fread(buf,1,len,f)!=len) { free(buf); return NULL; }
    return buf;
}

/* ── Write one row to a FILE* (used by WAL and full persist) */
static void write_row(FILE *f, const Row *row) {
    write_u64(f,(uint64_t)row->expires_at);
    for (int c=0;c<row->ncols;c++) {
        const Value *v=&row->vals[c];
        write_u32(f,(uint32_t)v->type);
        write_u32(f,(uint32_t)v->is_null);
        if (!v->is_null) {
            if (v->type==TYPE_INT)         write_u64(f,(uint64_t)v->v.ival);
            else if (v->type==TYPE_DECIMAL){ double d=v->v.dval; fwrite(&d,8,1,f); }
            else                            write_str(f,v->v.sval);
        }
    }
}

/* ── Read one row from FILE* into heap-allocated vals ───── */
static int read_row(FILE *f, int ncols, Row *row) {
    uint64_t exp=0;
    if (read_u64(f,&exp)!=0) return -1;
    row->expires_at=(time_t)exp;
    row->ncols=ncols;
    row->next=NULL;
    row->vals=calloc(ncols,sizeof(Value));
    for (int c=0;c<ncols;c++) {
        Value *v=&row->vals[c]; uint32_t u;
        if(read_u32(f,&u)!=0) goto fail; v->type=(ColType)u;
        if(read_u32(f,&u)!=0) goto fail; v->is_null=(int)u;
        if(!v->is_null) {
            if(v->type==TYPE_INT){uint64_t iv=0;if(read_u64(f,&iv)!=0)goto fail;v->v.ival=(long long)iv;}
            else if(v->type==TYPE_DECIMAL){if(fread(&v->v.dval,8,1,f)!=1)goto fail;}
            else{v->v.sval=read_str(f);if(!v->v.sval)goto fail;}
        }
    }
    return 0;
fail:
    for(int c=0;c<ncols;c++) value_free_inner(&row->vals[c]);
    free(row->vals); row->vals=NULL;
    return -1;
}

/* ── Fast WAL append (keep file handle open) ─────────────── */
int fast_append_row(Catalog *cat, const char *table_name, const Row *row) {
    Table *t=catalog_find_table(cat,table_name);
    if (!t) return -1;
    /* Open WAL file handle on first use */
    if (!t->wal_fp) {
        char path[768];
        snprintf(path,sizeof(path),"%s/tables/%s.wal",cat->data_dir,table_name);
        t->wal_fp=fopen(path,"ab");
        if (!t->wal_fp) return -1;
    }
    write_row(t->wal_fp, row);
    fflush(t->wal_fp);   /* flush to OS buffer — no fsync, so ~0ms overhead */
    return 0;
}

/* ── Drop table ──────────────────────────────────────────── */
int catalog_drop_table(Catalog *cat, const char *name, char *err) {
    pthread_mutex_lock(&cat->mutex);
    Table **pp=&cat->tables;
    while (*pp) {
        if (strcasecmp((*pp)->schema.name,name)==0) {
            Table *dead=*pp; *pp=dead->next;
            pthread_mutex_unlock(&cat->mutex);
            if (dead->wal_fp) { fclose(dead->wal_fp); dead->wal_fp=NULL; }
            Page *pg=dead->head;
            while (pg) {
                Page *pnxt=pg->next;
                for (int i=0;i<pg->nrows;i++) if (pg->rows[i].vals) row_free_vals(&pg->rows[i]);
                free(pg); pg=pnxt;
            }
            pthread_rwlock_destroy(&dead->lock);
            free(dead);
            char path[768];
            snprintf(path,sizeof(path),"%s/tables/%s.fql",cat->data_dir,name); remove(path);
            snprintf(path,sizeof(path),"%s/tables/%s.wal",cat->data_dir,name); remove(path);
            return 0;
        }
        pp=&(*pp)->next;
    }
    pthread_mutex_unlock(&cat->mutex);
    if (err) snprintf(err,256,"Table '%s' not found",name);
    return -1;
}

/* ── Full persist (snapshot) ─────────────────────────────── */
static int persist_table(Catalog *cat, Table *t, char *err) {
    /* Close WAL so we don't have split writes */
    if (t->wal_fp) { fflush(t->wal_fp); fclose(t->wal_fp); t->wal_fp=NULL; }

    char path[768];
    snprintf(path,sizeof(path),"%s/tables/%s.fql",cat->data_dir,t->schema.name);
    FILE *f=fopen(path,"wb");
    if (!f) { if(err) snprintf(err,256,"Cannot open: %s",strerror(errno)); return -1; }

    write_u32(f,(uint32_t)t->schema.ncols);
    write_u32(f,(uint32_t)(t->schema.pk_col+1));
    write_str(f,t->schema.name);
    for (int i=0;i<t->schema.ncols;i++) {
        ColDef *cd=&t->schema.cols[i];
        write_str(f,cd->name);
        write_u32(f,(uint32_t)cd->type);
        write_u32(f,(uint32_t)cd->primary_key);
        write_u32(f,(uint32_t)cd->not_null);
        write_u32(f,(uint32_t)cd->varchar_len);
    }
    write_u64(f,(uint64_t)t->total_rows);
    for (Page *pg=t->head;pg;pg=pg->next)
        for (int r=0;r<pg->nrows;r++)
            write_row(f,&pg->rows[r]);
    fclose(f);

    /* Remove WAL after successful full persist */
    char wal_path[768];
    snprintf(wal_path,sizeof(wal_path),"%s/tables/%s.wal",cat->data_dir,t->schema.name);
    remove(wal_path);
    return 0;
}

int catalog_persist(Catalog *cat, char *err) {
    for (Table *t=cat->tables;t;t=t->next) {
        pthread_rwlock_rdlock(&t->lock);
        int rc=persist_table(cat,t,err);
        pthread_rwlock_unlock(&t->lock);
        if (rc!=0) return rc;
    }
    return 0;
}

/* ── Load from disk ──────────────────────────────────────── */
int catalog_load(Catalog *cat, char *err) {
    (void)err;
    char tdir[600]; snprintf(tdir,sizeof(tdir),"%s/tables",cat->data_dir);
    char cmd[700];  snprintf(cmd,sizeof(cmd),"ls %s/*.fql 2>/dev/null",tdir);
    FILE *ls=popen(cmd,"r"); if (!ls) return 0;
    char path[512];
    while (fgets(path,sizeof(path),ls)) {
        path[strcspn(path,"\n")]='\0';
        FILE *f=fopen(path,"rb"); if (!f) continue;

        Schema schema; memset(&schema,0,sizeof(schema));
        uint32_t ncols,pk_col_enc;
        if (read_u32(f,&ncols)!=0||read_u32(f,&pk_col_enc)!=0) { fclose(f); continue; }
        schema.ncols=(int)ncols; schema.pk_col=(int)pk_col_enc-1;
        char *tname=read_str(f); if (!tname) { fclose(f); continue; }
        snprintf(schema.name,MAX_TABLE_NAME,"%s",tname); free(tname);

        for (int i=0;i<schema.ncols;i++) {
            ColDef *cd=&schema.cols[i]; uint32_t u;
            char *cname=read_str(f);
            if (!cname) { fclose(f); goto next_file; }
            snprintf(cd->name,MAX_COL_NAME,"%s",cname); free(cname);
            if(read_u32(f,&u)!=0){fclose(f);goto next_file;} cd->type=(ColType)u;
            if(read_u32(f,&u)!=0){fclose(f);goto next_file;} cd->primary_key=(int)u;
            if(read_u32(f,&u)!=0){fclose(f);goto next_file;} cd->not_null=(int)u;
            if(read_u32(f,&u)!=0){fclose(f);goto next_file;} cd->varchar_len=(int)u;
        }

        catalog_create_table(cat,&schema,NULL);
        Table *t=catalog_find_table(cat,schema.name);
        if (!t) { fclose(f); continue; }

        uint64_t total_rows=0; read_u64(f,&total_rows);
        for (uint64_t rn=0;rn<total_rows;rn++) {
            Row row; row.ncols=schema.ncols;
            if (read_row(f,schema.ncols,&row)!=0) break;
            table_insert_row(t,&row,NULL);
            for(int c=0;c<schema.ncols;c++) value_free_inner(&row.vals[c]);
            free(row.vals);
        }
        fclose(f);

        /* Replay WAL */
        char wal_path[768];
        snprintf(wal_path,sizeof(wal_path),"%s/tables/%s.wal",cat->data_dir,schema.name);
        FILE *wf=fopen(wal_path,"rb");
        if (wf) {
            Row wrow; wrow.ncols=schema.ncols;
            while (read_row(wf,schema.ncols,&wrow)==0) {
                table_insert_row(t,&wrow,NULL);
                for(int c=0;c<schema.ncols;c++) value_free_inner(&wrow.vals[c]);
                free(wrow.vals);
            }
            fclose(wf);
        }
        continue;
        next_file: fclose(f);
    }
    pclose(ls); return 0;
}
