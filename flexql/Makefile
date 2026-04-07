# FlexQL Makefile
CC      = gcc
CXX     = g++
CFLAGS  = -O2 -Wall -Wextra -Iinclude -Iinclude/common -Iinclude/network \
           -Iinclude/parser -Iinclude/storage -Iinclude/index \
           -Iinclude/cache -Iinclude/query
CXXFLAGS = $(CFLAGS) -std=c++17
LDFLAGS  = -lpthread -lm

SERVER_SRCS = \
    src/parser/parser.c \
    src/storage/storage.c \
    src/index/index.c \
    src/cache/cache.c \
    src/query/query.c \
    src/server/server.c

CLIENT_SRCS = \
    src/client/repl.c

# Benchmark target (C++ – links against the client C library)
BENCH_SRCS = \
    src/parser/parser.c \
    src/storage/storage.c \
    src/index/index.c \
    src/cache/cache.c \
    src/query/query.c

.PHONY: all clean dirs server client lib benchmark

all: dirs server client lib

dirs:
	@mkdir -p bin build data/tables data/indexes data/wal

server: $(SERVER_SRCS)
	$(CC) $(CFLAGS) -o bin/flexql-server $^ $(LDFLAGS)
	@echo "[✓] bin/flexql-server built"

client: $(CLIENT_SRCS)
	$(CC) $(CFLAGS) -o bin/flexql-client $^ $(LDFLAGS)
	@echo "[✓] bin/flexql-client built"

# Shared library for benchmark_flexql.cpp
lib: $(BENCH_SRCS) src/client/client.c
	$(CC) $(CFLAGS) -fPIC -shared \
	    src/client/client.c \
	    src/parser/parser.c \
	    src/storage/storage.c \
	    src/index/index.c \
	    src/cache/cache.c \
	    src/query/query.c \
	    -o bin/libflexql.so $(LDFLAGS)
	@echo "[✓] bin/libflexql.so built"

# Compile the benchmark provided by the course
benchmark: bin/libflexql.so
	$(CXX) $(CXXFLAGS) -Iinclude \
	    tests/benchmark_flexql.cpp \
	    -Lbin -lflexql -Wl,-rpath,./bin \
	    -o bin/benchmark $(LDFLAGS)
	@echo "[✓] bin/benchmark built"

clean:
	rm -rf bin/* build/*

# Quick smoke test (requires server to be running)
test:
	@echo "=== Smoke test ==="
	@echo "CREATE TABLE T(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(64) NOT NULL);" \
	    | ./bin/flexql-client 127.0.0.1 9000
	@echo "INSERT INTO T VALUES(1,'Alice');" \
	    | ./bin/flexql-client 127.0.0.1 9000
	@echo "INSERT INTO T VALUES(2,'Bob');" \
	    | ./bin/flexql-client 127.0.0.1 9000
	@echo "SELECT * FROM T;" \
	    | ./bin/flexql-client 127.0.0.1 9000
	@echo "SELECT * FROM T WHERE ID = 1;" \
	    | ./bin/flexql-client 127.0.0.1 9000
	@echo "=== Done ==="
