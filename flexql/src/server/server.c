/*
 * server.c - FlexQL high-performance TCP server.
 *
 * Architecture: Thread Pool (fixed N worker threads) + accept queue.
 *
 * Why thread pool over thread-per-connection:
 *   Thread-per-connection: O(N) threads for N clients, high creation overhead,
 *     memory per thread, OS scheduler thrash at high concurrency.
 *   Thread pool: Fixed M threads (M = CPU cores), zero creation overhead,
 *     bounded memory, better cache locality, handles N >> M connections.
 *
 * Usage: ./flexql-server [port] [data_dir] [threads]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>

#include "../include/parser/parser.h"
#include "../include/storage/storage.h"
#include "../include/index/index.h"
#include "../include/cache/cache.h"
#include "../include/query/query.h"
#include "../include/network/protocol.h"
#include "../include/flexql.h"
#include "../network/net.c"

/* ── Thread pool ──────────────────────────────────────────── */
#define POOL_DEFAULT_THREADS 8
#define QUEUE_CAPACITY       4096

typedef struct {
    int *fds;
    int  head, tail, count, cap;
    pthread_mutex_t mu;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
    int             shutdown;
} WorkQueue;

static WorkQueue  g_queue;
static pthread_t *g_workers;
static int        g_nworkers;

static void queue_init(WorkQueue *q, int cap) {
    q->fds   = calloc(cap, sizeof(int));
    q->cap   = cap;
    q->head  = q->tail = q->count = 0;
    q->shutdown = 0;
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full,  NULL);
}

static void queue_push(WorkQueue *q, int fd) {
    pthread_mutex_lock(&q->mu);
    while (q->count == q->cap && !q->shutdown)
        pthread_cond_wait(&q->not_full, &q->mu);
    if (!q->shutdown) {
        q->fds[q->tail] = fd;
        q->tail = (q->tail + 1) % q->cap;
        q->count++;
        pthread_cond_signal(&q->not_empty);
    }
    pthread_mutex_unlock(&q->mu);
}

static int queue_pop(WorkQueue *q) {
    pthread_mutex_lock(&q->mu);
    while (q->count == 0 && !q->shutdown)
        pthread_cond_wait(&q->not_empty, &q->mu);
    if (q->shutdown && q->count == 0) {
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    int fd = q->fds[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mu);
    return fd;
}

/* ── Global engine ────────────────────────────────────────── */
static Catalog      *g_catalog;
static IndexCatalog *g_idxcat;
static LRUCache     *g_cache;
static QueryEngine  *g_qe;
static int           g_server_fd = -1;

/* ── Per-client handler ───────────────────────────────────── */
typedef struct { int fd; int abort; } ClientCtx;

static int row_callback(void *data, int ncols, char **values, char **col_names) {
    ClientCtx *ctx = (ClientCtx*)data;
    if (ctx->abort) return 1;
    int fd = ctx->fd;
    if (send_u32(fd, PROTO_MSG_ROW)    != 0) { ctx->abort=1; return 1; }
    if (send_u32(fd, (uint32_t)ncols)  != 0) { ctx->abort=1; return 1; }
    for (int i=0;i<ncols;i++) if (send_str(fd,values[i])   !=0) { ctx->abort=1; return 1; }
    for (int i=0;i<ncols;i++) if (send_str(fd,col_names[i])!=0) { ctx->abort=1; return 1; }
    return 0;
}

static void handle_client(int fd) {
    while (1) {
        uint32_t sql_len;
        if (recv_u32(fd, &sql_len) != 0) break;
        if (sql_len == 0 || sql_len > 4*1024*1024) break;
        char *sql = calloc(sql_len+1, 1);
        if (read_exact(fd, sql, sql_len) != 0) { free(sql); break; }

        ParsedStmt stmt;
        char parse_err[512];
        int rc = parse_sql(sql, &stmt, parse_err, sizeof(parse_err));
        if (rc != 0) {
            send_u32(fd, PROTO_MSG_DONE);
            send_u32(fd, PROTO_MSG_ERROR);
            send_str(fd, parse_err);
            free(sql); continue;
        }
        ClientCtx ctx = {fd, 0};
        char *errmsg = NULL;
        int exec_rc = qe_execute(g_qe, sql, &stmt, row_callback, &ctx, &errmsg);
        send_u32(fd, PROTO_MSG_DONE);
        if (exec_rc == FLEXQL_OK && !ctx.abort) {
            send_u32(fd, PROTO_MSG_OK); send_u32(fd, 0);
        } else {
            send_u32(fd, PROTO_MSG_ERROR);
            send_str(fd, errmsg ? errmsg : "Unknown error");
        }
        if (errmsg) free(errmsg);
        free(sql);
    }
    close(fd);
}

/* ── Worker thread ────────────────────────────────────────── */
static void *worker(void *arg) {
    (void)arg;
    while (1) {
        int fd = queue_pop(&g_queue);
        if (fd < 0) break;
        handle_client(fd);
    }
    return NULL;
}

/* ── Signal handler ───────────────────────────────────────── */
static void handle_signal(int sig) {
    (void)sig;
    printf("\n[server] Shutting down...\n");
    if (g_catalog) catalog_persist(g_catalog, NULL);
    /* Wake all workers */
    pthread_mutex_lock(&g_queue.mu);
    g_queue.shutdown = 1;
    pthread_cond_broadcast(&g_queue.not_empty);
    pthread_mutex_unlock(&g_queue.mu);
    if (g_server_fd >= 0) close(g_server_fd);
    exit(0);
}

/* ── main ─────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int         port      = (argc > 1) ? atoi(argv[1]) : 9000;
    const char *data_dir  = (argc > 2) ? argv[2] : "./data";
    int         nthreads  = (argc > 3) ? atoi(argv[3]) : POOL_DEFAULT_THREADS;
    if (nthreads < 1) nthreads = 1;
    if (nthreads > 256) nthreads = 256;

    signal(SIGINT,  handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    /* Init subsystems */
    g_catalog = catalog_create(data_dir);
    g_idxcat  = idxcat_create();
    g_cache   = cache_create(CACHE_CAPACITY);
    g_qe      = qe_create(g_catalog, g_idxcat, g_cache);

    char load_err[256];
    if (catalog_load(g_catalog, load_err) != 0)
        fprintf(stderr, "[server] Warning: %s\n", load_err);
    else
        printf("[server] Data loaded from '%s'\n", data_dir);

    /* Rebuild indexes for loaded tables */
    for (Table *t = g_catalog->tables; t; t = t->next) {
        if (t->schema.pk_col >= 0) {
            HashIndex *idx = idxcat_get_or_create(g_idxcat, t->schema.name);
            for (Page *pg = t->head; pg; pg = pg->next) {
                for (int r = 0; r < pg->nrows; r++) {
                    char pk_str[256];
                    Row *row = &pg->rows[r];
                    /* value_to_str inline */
                    Value *v = &row->vals[t->schema.pk_col];
                    if (!v->is_null) {
                        switch(v->type) {
                            case TYPE_INT:     snprintf(pk_str,sizeof(pk_str),"%lld",(long long)v->v.ival); break;
                            case TYPE_DECIMAL: {
                                double d=v->v.dval;
                                if(d==(long long)d&&d>=-1e15&&d<=1e15) snprintf(pk_str,sizeof(pk_str),"%.0f",d);
                                else snprintf(pk_str,sizeof(pk_str),"%g",d);
                                break;
                            }
                            default: snprintf(pk_str,sizeof(pk_str),"%s",v->v.sval?v->v.sval:""); break;
                        }
                        index_insert(idx, pk_str, row);
                    }
                }
            }
        }
    }

    /* TCP socket */
    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_fd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);
    if (bind(g_server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(g_server_fd, 512) < 0) { perror("listen"); return 1; }

    /* Start thread pool */
    queue_init(&g_queue, QUEUE_CAPACITY);
    g_workers  = calloc(nthreads, sizeof(pthread_t));
    g_nworkers = nthreads;
    for (int i = 0; i < nthreads; i++)
        pthread_create(&g_workers[i], NULL, worker, NULL);

    printf("[server] FlexQL listening on port %d  (thread pool: %d workers)\n", port, nthreads);
    printf("[server] Data directory: %s\n", data_dir);

    while (1) {
        struct sockaddr_in cli; socklen_t cli_len = sizeof(cli);
        int cli_fd = accept(g_server_fd, (struct sockaddr*)&cli, &cli_len);
        if (cli_fd < 0) { if (errno == EINTR) break; continue; }
        queue_push(&g_queue, cli_fd);
    }

    catalog_persist(g_catalog, NULL);
    return 0;
}
