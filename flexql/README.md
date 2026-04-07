# FlexQL — A Flexible SQL-like Database Driver

A complete client-server relational database implemented from scratch in C,
fulfilling all requirements of the FlexQL Design Lab project.

## Features

- TCP client-server architecture with multithreaded server
- SQL support: `CREATE TABLE`, `INSERT`, `SELECT`, `WHERE`, `INNER JOIN`
- Column types: `INT`, `DECIMAL`, `VARCHAR`
- WHERE/JOIN operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Primary key indexing via FNV hash-map (O(1) PK lookup)
- LRU query result cache (invalidated on write)
- Persistent storage — binary `.fql` files survive restarts
- Row expiration timestamps
- Clean Client C API: `flexql_open`, `flexql_close`, `flexql_exec`, `flexql_free`
- Interactive REPL terminal
- Linkable as a shared library (`libflexql.so`) for benchmark integration

---

## Project Structure

```
flexql/
├── Makefile
├── README.md
├── DESIGN.md
├── flexql.h                      ← root alias (re-exports include/flexql.h)
├── include/
│   ├── flexql.h                  ← Public API header
│   ├── common/types.h            ← Shared type definitions
│   ├── network/protocol.h        ← Wire protocol constants
│   ├── parser/parser.h
│   ├── storage/storage.h
│   ├── index/index.h
│   ├── cache/cache.h
│   └── query/query.h
├── src/
│   ├── parser/parser.c           ← Recursive-descent SQL parser
│   ├── storage/storage.c         ← Row-major heap + binary persistence
│   ├── index/index.c             ← FNV hash-map primary key index
│   ├── cache/cache.c             ← LRU query cache
│   ├── query/query.c             ← Query executor
│   ├── network/net.c             ← TCP framing helpers
│   ├── server/server.c           ← Multithreaded TCP server
│   └── client/
│       ├── client.c              ← flexql_open/close/exec/free
│       └── repl.c                ← Interactive REPL
├── tests/
│   └── benchmark_flexql.cpp      ← (place here from course repo)
├── bin/                          ← Compiled binaries
└── data/
    └── tables/                   ← Persistent .fql table files
```

---

## Build

```bash
# Build everything (server, client, shared library)
make all

# Or individually:
make server     # → bin/flexql-server
make client     # → bin/flexql-client
make lib        # → bin/libflexql.so

# Build benchmark (after placing benchmark_flexql.cpp in tests/)
make benchmark  # → bin/benchmark
```

Requirements: `gcc`, `g++`, `make`, `pthread`, `libm`

---

## Running

### Start the server

```bash
./bin/flexql-server [port] [data_dir]
# defaults: port=9000, data_dir=./data
./bin/flexql-server 9000 ./data
```

### Interactive REPL

```bash
./bin/flexql-client <host> <port>
./bin/flexql-client 127.0.0.1 9000
```

```bash
Run All Automated Tests (from tests/ folder)
cd tests/
bash run_tests.sh
bash test_sql_features.sh
bash test_datatypes.sh
bash test_index_cache.sh
bash test_concurrency.sh
```

```
Connected to FlexQL server
flexql> CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, FIRST_NAME VARCHAR(64) NOT NULL, LAST_NAME VARCHAR(64) NOT NULL, EMAIL VARCHAR(128) NOT NULL);
flexql> INSERT INTO STUDENT VALUES(1,'Alice','Smith','alice@example.com');
flexql> INSERT INTO STUDENT VALUES(2,'Bob','Jones','bob@example.com');
flexql> SELECT * FROM STUDENT;
ID = 1
FIRST_NAME = Alice
LAST_NAME = Smith
EMAIL = alice@example.com

flexql> SELECT FIRST_NAME, EMAIL FROM STUDENT WHERE ID = 1;
FIRST_NAME = Alice
EMAIL = alice@example.com

flexql> .exit
Connection closed
```

### Run benchmark

```bash
# Place benchmark_flexql.cpp from https://github.com/Bivas-Biswas/FlexQL_Benchmark_Unit_Tests
# into the tests/ directory, then:
make benchmark
./bin/benchmark 127.0.0.1 9000
```

---

## Supported SQL

### CREATE TABLE
```sql
CREATE TABLE table_name (
    col1 INT PRIMARY KEY NOT NULL,
    col2 VARCHAR(128) NOT NULL,
    col3 DECIMAL
);
```
Types: `INT`, `DECIMAL`, `VARCHAR`

### INSERT
```sql
INSERT INTO table_name VALUES(value1, value2, value3);
```

### SELECT
```sql
-- All columns
SELECT * FROM table_name;

-- Specific columns
SELECT col1, col2 FROM table_name;

-- With WHERE (operators: = != < > <= >=)
SELECT * FROM table_name WHERE col1 = value;
SELECT * FROM table_name WHERE col1 >= 100;

-- INNER JOIN with optional WHERE
SELECT * FROM tableA INNER JOIN tableB ON tableA.col = tableB.col;
SELECT * FROM tableA INNER JOIN tableB ON tableA.col = tableB.col WHERE tableA.id = 1;
```

---

## Client API

```c
#include "include/flexql.h"

// Open connection
FlexQL *db;
int rc = flexql_open("127.0.0.1", 9000, &db);

// Execute SQL (callback called once per result row)
int my_callback(void *data, int ncols, char **values, char **col_names) {
    for (int i = 0; i < ncols; i++)
        printf("%s = %s\n", col_names[i], values[i]);
    return 0; // 0=continue, 1=abort
}

char *errmsg = NULL;
rc = flexql_exec(db, "SELECT * FROM STUDENT;", my_callback, NULL, &errmsg);
if (rc != FLEXQL_OK) {
    fprintf(stderr, "Error: %s\n", errmsg);
    flexql_free(errmsg);
}

// Close
flexql_close(db);
```

---

## Verifying Requirements

| Requirement                      | How to verify                                      |
|----------------------------------|----------------------------------------------------|
| CREATE TABLE                     | Run `CREATE TABLE T(ID INT PRIMARY KEY NOT NULL);` |
| INSERT                           | Run `INSERT INTO T VALUES(1);`                     |
| SELECT *                         | Run `SELECT * FROM T;`                             |
| SELECT cols                      | Run `SELECT ID FROM T;`                            |
| WHERE (all 6 operators)          | Run `SELECT * FROM T WHERE ID >= 1;`               |
| INNER JOIN                       | Create 2 tables, run JOIN query                    |
| Persistent storage               | Insert rows, restart server, SELECT again          |
| Primary key index                | WHERE on PK uses O(1) hash lookup                  |
| LRU cache                        | Cache built per SELECT; invalidated on INSERT      |
| Multithreaded server             | Connect multiple clients simultaneously            |
| flexql_open/close/exec/free API  | See client.c / repl.c                             |
| Benchmark integration            | `make benchmark` → `./bin/benchmark 127.0.0.1 9000`|
