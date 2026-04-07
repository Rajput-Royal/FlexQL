/*
 * repl.c – Interactive FlexQL client REPL.
 *
 * Usage: ./flexql-client <host> <port>
 *   e.g. ./flexql-client 127.0.0.1 9000
 *
 * When stdin is a pipe the client runs in non-interactive mode:
 * it reads all SQL statements delimited by ';', executes each,
 * then exits — keeping the connection alive for the full session.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Include the client library directly (single-translation-unit build) */
#include "client.c"

/* ── Row display callback ─────────────────────────────────── */
static int print_row(void *data, int ncols, char **values, char **col_names) {
    (void)data;
    for (int i = 0; i < ncols; i++)
        printf("%s = %s\n", col_names[i], values[i] ? values[i] : "NULL");
    printf("\n");
    fflush(stdout);
    return 0; /* continue */
}

/* ── main ─────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);

    FlexQL *db = NULL;
    int rc = flexql_open(host, port, &db);
    if (rc != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL server at %s:%d\n", host, port);
        return 1;
    }

    int interactive = isatty(fileno(stdin));
    if (interactive) printf("Connected to FlexQL server\n");

    /* Accumulator: collect text until we have a full statement (ends with ';') */
    char  acc[1024 * 256];
    int   acc_len = 0;
    acc[0] = '\0';

    char buf[4096];

    while (1) {
        if (interactive) { printf("flexql> "); fflush(stdout); }

        if (!fgets(buf, sizeof(buf), stdin)) {
            /* EOF: execute any remaining accumulated SQL */
            if (acc_len > 0) {
                char *errmsg = NULL;
                rc = flexql_exec(db, acc, print_row, NULL, &errmsg);
                if (rc != FLEXQL_OK) {
                    fprintf(stderr, "SQL error: %s\n", errmsg ? errmsg : "unknown");
                    flexql_free(errmsg);
                }
            }
            break;
        }

        /* Strip trailing newline */
        buf[strcspn(buf, "\n")] = '\0';

        /* Built-in meta-commands */
        if (strcmp(buf, ".exit") == 0 || strcmp(buf, "exit") == 0 ||
            strcmp(buf, "quit")  == 0 || strcmp(buf, ".quit") == 0) {
            if (interactive) printf("Connection closed\n");
            break;
        }
        if (strcmp(buf, ".help") == 0) {
            printf("Commands:\n"
                   "  .exit / .quit    Disconnect\n"
                   "  .help            Show this message\n"
                   "  Any SQL;         Sent to server\n");
            continue;
        }
        if (buf[0] == '\0') continue; /* blank line */

        /* Append to accumulator */
        int chunk = (int)strlen(buf);
        if (acc_len + chunk + 2 < (int)sizeof(acc)) {
            memcpy(acc + acc_len, buf, chunk);
            acc_len += chunk;
            acc[acc_len++] = ' ';
            acc[acc_len]   = '\0';
        }

        /* Execute each complete statement (split on ';') */
        while (1) {
            char *semi = strchr(acc, ';');
            if (!semi) break;

            /* Terminate at the semicolon */
            *semi = '\0';

            /* Trim leading whitespace */
            char *stmt = acc;
            while (*stmt == ' ' || *stmt == '\t' || *stmt == '\n') stmt++;

            if (*stmt) {
                char *errmsg = NULL;
                int exec_rc = flexql_exec(db, stmt, print_row, NULL, &errmsg);
                if (exec_rc != FLEXQL_OK) {
                    fprintf(stderr, "SQL error: %s\n", errmsg ? errmsg : "unknown");
                    flexql_free(errmsg);
                }
            }

            /* Shift remaining text to front of accumulator */
            char *after = semi + 1;
            acc_len = (int)strlen(after);
            memmove(acc, after, acc_len + 1);
        }
    }

    flexql_close(db);
    return 0;
}
