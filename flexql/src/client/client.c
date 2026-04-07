/*
 * client.c – FlexQL client library.
 * Implements flexql_open, flexql_close, flexql_exec, flexql_free.
 */

#include "../include/flexql.h"
#include "../include/network/protocol.h"
#include "../network/net.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

/* Internal structure behind the opaque FlexQL * handle */
struct FlexQL {
    int  sock_fd;
};

/* ── flexql_open ──────────────────────────────────────────── */
int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return FLEXQL_ERROR;

    struct hostent *he = gethostbyname(host);
    if (!he) { close(fd); return FLEXQL_ERROR; }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return FLEXQL_ERROR;
    }

    FlexQL *h = calloc(1, sizeof(FlexQL));
    h->sock_fd = fd;
    *db = h;
    return FLEXQL_OK;
}

/* ── flexql_close ─────────────────────────────────────────── */
int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    close(db->sock_fd);
    free(db);
    return FLEXQL_OK;
}

/* ── flexql_exec ──────────────────────────────────────────── */
int flexql_exec(FlexQL *db, const char *sql,
                int (*callback)(void*, int, char**, char**),
                void *arg, char **errmsg)
{
    if (!db || !sql) {
        if (errmsg) *errmsg = strdup("Invalid handle or SQL");
        return FLEXQL_ERROR;
    }

    int fd = db->sock_fd;

    /* Send SQL */
    uint32_t sql_len = (uint32_t)strlen(sql);
    if (send_u32(fd, sql_len) != 0 ||
        write_exact(fd, sql, sql_len) != 0) {
        if (errmsg) *errmsg = strdup("Send failed");
        return FLEXQL_ERROR;
    }

    /* Read server responses */
    while (1) {
        uint32_t msg_type;
        if (recv_u32(fd, &msg_type) != 0) {
            if (errmsg) *errmsg = strdup("Connection lost");
            return FLEXQL_ERROR;
        }

        if (msg_type == PROTO_MSG_ROW) {
            uint32_t ncols;
            if (recv_u32(fd, &ncols) != 0) return FLEXQL_ERROR;

            char **values = calloc(ncols, sizeof(char*));
            char **names  = calloc(ncols, sizeof(char*));

            for (uint32_t i = 0; i < ncols; i++) {
                values[i] = recv_str(fd);
                if (!values[i]) values[i] = strdup("NULL");
            }
            for (uint32_t i = 0; i < ncols; i++) {
                names[i] = recv_str(fd);
                if (!names[i]) names[i] = strdup("?");
            }

            if (callback)
                callback(arg, (int)ncols, values, names);

            for (uint32_t i = 0; i < ncols; i++) { free(values[i]); free(names[i]); }
            free(values); free(names);

        } else if (msg_type == PROTO_MSG_DONE) {
            /* Next message is OK or ERROR */
            uint32_t result;
            if (recv_u32(fd, &result) != 0) return FLEXQL_ERROR;

            if (result == PROTO_MSG_OK) {
                uint32_t dummy; recv_u32(fd, &dummy);
                return FLEXQL_OK;
            } else { /* PROTO_MSG_ERROR */
                char *msg = recv_str(fd);
                if (errmsg) *errmsg = msg;
                else free(msg);
                return FLEXQL_ERROR;
            }
        } else {
            if (errmsg) *errmsg = strdup("Unexpected message type");
            return FLEXQL_ERROR;
        }
    }
}

/* ── flexql_free ──────────────────────────────────────────── */
void flexql_free(void *ptr) { free(ptr); }
