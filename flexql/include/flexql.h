#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

/* ── Error codes ─────────────────────────────────────────── */
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

/* ── Opaque database handle ──────────────────────────────── */
typedef struct FlexQL FlexQL;

/* ── Public API ──────────────────────────────────────────── */

/**
 * Opens a TCP connection to a running FlexQL server.
 * @param host  Server hostname or IP (e.g. "127.0.0.1")
 * @param port  Server port            (e.g. 9000)
 * @param db    Out-param: initialised handle on success
 * @return FLEXQL_OK or FLEXQL_ERROR
 */
int flexql_open(const char *host, int port, FlexQL **db);

/**
 * Closes the connection and frees all resources.
 */
int flexql_close(FlexQL *db);

/**
 * Sends an SQL statement to the server and invokes callback for every
 * result row.
 *
 * Callback prototype:
 *   int cb(void *data, int columnCount, char **values, char **columnNames);
 *   Return 0 to continue, 1 to abort.
 *
 * @param db        Open connection
 * @param sql       SQL text (NUL-terminated)
 * @param callback  Called once per result row, or NULL
 * @param arg       Forwarded as first arg to callback
 * @param errmsg    On error, *errmsg is set to a malloc'd string; free with flexql_free()
 */
int flexql_exec(
    FlexQL       *db,
    const char   *sql,
    int         (*callback)(void*, int, char**, char**),
    void         *arg,
    char        **errmsg
);

/**
 * Frees memory allocated by the FlexQL library (e.g. errmsg strings).
 */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* FLEXQL_H */
