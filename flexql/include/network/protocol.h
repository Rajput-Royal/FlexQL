#ifndef FLEXQL_PROTOCOL_H
#define FLEXQL_PROTOCOL_H

#include <stdint.h>

/*
 * Wire protocol (all integers in network byte order):
 *
 *  Client → Server:
 *    [4-byte SQL length][SQL text (no NUL)]
 *
 *  Server → Client  (for each result row):
 *    [4-byte msg_type]
 *    MSG_ROW   → [4-byte ncols] then ncols × [4-byte len][value bytes]
 *                                  then ncols × [4-byte len][colname bytes]
 *    MSG_OK    → [4-byte 0]
 *    MSG_ERROR → [4-byte errmsg_len][errmsg bytes]
 *    MSG_DONE  → signals end of result set, followed by MSG_OK or MSG_ERROR
 */

#define PROTO_MSG_ROW   0x01
#define PROTO_MSG_OK    0x02
#define PROTO_MSG_ERROR 0x03
#define PROTO_MSG_DONE  0x04

#endif /* FLEXQL_PROTOCOL_H */
