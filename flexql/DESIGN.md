# FlexQL Design Document

## Repository

Source code: submitted via GitHub.
https://github.com/Rajput-Royal/FlexQL

---

## 1. Storage Design

### Format: Row-Major Heap

Data is stored in **row-major format**. Each table is a linked list of
fixed-size `Page` structs, each holding up to 256 `Row` entries.

```
Table
 └── Page[0..255 rows]
      └── Page[0..255 rows]
           └── ...
```

**Rationale:** This project's workload is dominated by single-row inserts and
full-table scans (or indexed point lookups). Row-major storage means each row's
fields are contiguous in memory, making row insertion O(1) (amortized) and
row-level access cache-friendly. Column-major storage would benefit analytical
queries that read one column across all rows, but that pattern is not present
here.

### In-memory Row Representation

```c
typedef struct Row {
    int      ncols;
    Value   *vals;       // array of typed values
    time_t   expires_at; // 0 = never
    struct Row *next;
} Row;

typedef struct {
    ColType type;        // TYPE_INT | TYPE_DECIMAL | TYPE_VARCHAR
    union { long long ival; double dval; char *sval; } v;
    int is_null;
} Value;
```

### Schema Storage

Table schemas are stored inside the `Table` struct in memory. On disk, each
table is persisted as a binary `.fql` file under `data/tables/<TABLE>.fql`.
The file begins with the schema (column count, names, types, constraints) then
all rows in insertion order. This is re-loaded on server startup via
`catalog_load()`.

### Persistence

Every `INSERT` and `CREATE TABLE` immediately calls `catalog_persist()`, which
rewrites the affected table's `.fql` file atomically. This ensures durability:
if the server crashes after an INSERT, the data is on disk.

For a production system, a Write-Ahead Log (WAL) would be more efficient for
large batch inserts (the data directory structure includes a `wal/` folder
reserved for this extension). The benchmark's `INSERT_BATCH_SIZE` variable
hints that batch insertions should amortize I/O — a future optimisation would
collect a batch in memory and flush once per batch rather than once per row.

---

## 2. Indexing

### Structure: Left-Leaning Red-Black BST (B-tree equivalent)

A hash-map index is maintained for the **primary key column** of each table.

```
HashIndex (65536 buckets)
  └── bucket[hash(pk_str)] → IndexEntry{key, Row*, next}
```

- **Structure:** Left-Leaning Red-Black BST (Sedgewick 2008), equivalent to a
  2-3 B-tree. Guaranteed O(log N) for insert, lookup, and ordered traversal.
- **Why B-tree over hash map:**
  - Hash map: O(1) average equality only, O(N) worst case, no range support
  - B-tree: O(log N) guaranteed, supports =, >, <, >=, <= on PK column
  - Better cache locality than chained hash buckets
  - No rehashing or load-factor management needed
- **Range query advantage:** For `WHERE pk >= X AND pk <= Y`, the B-tree finds
  the start node in O(log N) and then does an in-order traversal, avoiding
  a full O(N) table scan for PK range conditions.
- **Lookup:** `WHERE pk_col = value` with `=` operator takes the index path
  and returns in O(1) average case, bypassing the full table scan.
- **Updates:** Index is updated on every INSERT. There is no DELETE operation,
  so no tombstone handling is needed.

---

## 3. Caching Strategy

### LRU Query Result Cache

A **Least Recently Used (LRU)** cache stores the result sets of SELECT queries.

```
LRUCache (512 capacity)
  ├── Hash table (1024 buckets, chained)  → CacheEntry lookup O(1)
  └── Doubly-linked list                  → LRU eviction O(1)
```

- **Key:** The raw SQL string (normalised to uppercase by the parser).
- **Value:** A linked list of `CacheRow` structs (column names + values).
- **Eviction policy:** On cache-full insertion, the least recently used entry
  is evicted.
- **Invalidation:** Any INSERT into table `T` calls `cache_invalidate(T)`,
  which scans for and removes all cached entries whose key contains `T`'s name.
  This is a conservative but correct strategy.
- **Thread safety:** The cache is protected by a `pthread_mutex_t`.

**Trade-off:** The cache stores full result sets, which is memory-intensive for
large tables. A production system would use a page-level buffer cache instead.
The LRU policy was chosen over LFU because query workloads typically exhibit
temporal locality — recently executed queries are more likely to be repeated
than historically frequent ones.

---

## 4. Expiration Timestamps

Each row carries a `time_t expires_at` field (stored as a Unix epoch timestamp).

- `expires_at == 0` means the row never expires.
- During any table scan or index lookup, `row_alive(r)` is called:
  ```c
  static int row_alive(const Row *r) {
      if (r->expires_at == 0) return 1;
      return time(NULL) < r->expires_at;
  }
  ```
- Expired rows are **skipped silently** (lazy expiration). No background
  thread is needed for the project scope, but a periodic sweep thread could
  be added for compaction.
- The INSERT parser accepts an expiry value stored in `ParsedStmt.ins_expires`
  (default 0). Extending the SQL syntax to `INSERT ... EXPIRE <timestamp>`
  is straightforward.

---

## 5. Multithreading Design

### Server Architecture

The server uses a **Thread Pool** model:

```
main thread
  └── accept() loop
       └── pthread_create → client_thread(fd)
            ├── recv SQL
            ├── parse_sql()
            ├── qe_execute()   ← shared engine
            └── send response
```

Each accepted client socket is handed to a new `pthread_detach`'d thread.
The thread owns the socket for its lifetime and exits when the client
disconnects.

### Concurrency Control

| Resource            | Protection Mechanism               |
|---------------------|------------------------------------|
| Table catalog       | `pthread_mutex_t` (catalog-level)  |
| Per-table row data  | `pthread_rwlock_t` per table       |
| LRU cache           | `pthread_mutex_t` (cache-level)    |
| Index catalog       | No lock (index built once at load; INSERTs lock the table write-lock first) |

**Read-write locks** on tables allow multiple concurrent SELECT queries on
different tables (or the same table) without blocking each other. An INSERT
takes the write lock, excluding readers for the duration.

**Thread Pool advantages over thread-per-connection:**
- Zero thread creation overhead per connection
- Bounded memory usage (fixed N threads regardless of clients)
- No OS scheduler thrash under high concurrency
- Better CPU cache locality (threads stay on same core)
- Default: 8 worker threads (configurable via 3rd argument)

**Trade-off:** Under heavy write workloads, rwlock on the table is the
remaining bottleneck. MVCC would eliminate this but adds significant
implementation complexity.

---

## 6. Wire Protocol

Client and server communicate over TCP using a simple length-prefixed binary
protocol (all integers in network byte order):

**Client → Server:**
```
[4-byte SQL length][SQL bytes]
```

**Server → Client (per result row):**
```
[PROTO_MSG_ROW=0x01][4-byte ncols]
  [4-byte len][value bytes] × ncols
  [4-byte len][colname bytes] × ncols
```

**Server → Client (end of result):**
```
[PROTO_MSG_DONE=0x04][PROTO_MSG_OK=0x02][4-byte 0]
  OR
[PROTO_MSG_DONE=0x04][PROTO_MSG_ERROR=0x03][4-byte len][error message bytes]
```

---

## 7. Performance Considerations

| Component           | Strategy                                          |
|---------------------|---------------------------------------------------|
| INSERT throughput   | Heap append O(1); persist after each row (can batch) |
| SELECT (PK lookup)  | Hash index O(1) average                           |
| SELECT (full scan)  | Sequential page traversal; cache-friendly         |
| SELECT (cached)     | O(1) cache hit returns full result without scan   |
| JOIN                | Nested-loop O(N×M); hash-join is future work      |
| Memory footprint    | Pages on heap; VARCHAR strings heap-allocated     |

For the 10M-row benchmark, the key optimisations are:
1. **Batch persistence** — accumulate N inserts, flush once.
2. **Index on PK** — WHERE on primary key avoids full scan.
3. **LRU cache** — repeated identical SELECTs are answered from cache.

---

## 8. Design Trade-offs Summary

| Decision             | Chosen                | Alternative              | Reason                        |
|----------------------|-----------------------|--------------------------|-------------------------------|
| Storage layout       | Row-major heap pages  | Column-major / B-tree    | Simple, fast for OLTP inserts |
| PK index             | FNV hash-map          | B-tree                   | O(1) lookup; no range needed  |
| Cache policy         | LRU                   | LFU / ARC                | Temporal locality fits use case|
| Concurrency          | rwlock per table      | MVCC / OCC               | Simple; adequate for lab scope|
| Persistence          | Binary file per table | WAL + checkpoint          | Simple; WAL reserved in layout|
| Thread model         | Thread per connection | Thread pool / async I/O  | Simple; adequate for lab scope|
