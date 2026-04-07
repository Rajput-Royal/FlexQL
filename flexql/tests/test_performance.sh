#!/usr/bin/env bash
# Q6 & Q7: Performance Evaluation (uses batch INSERT)
# Usage: bash tests/test_performance.sh [host] [port] [rows]
HOST=${1:-127.0.0.1}; PORT=${2:-9000}; ROWS=${3:-10000}

python3 << PYEOF
import socket, struct, time, sys, os

HOST="$HOST"; PORT=$PORT; TARGET_ROWS=$ROWS
BATCH_SIZE=500  # INSERT_BATCH_SIZE equivalent

def xact(s, sql):
    b=sql.encode(); s.send(struct.pack(">I",len(b))+b)
    s.settimeout(60); rows=[]
    while True:
        hdr=s.recv(4)
        if not hdr: return False,"closed"
        mt=struct.unpack(">I",hdr)[0]
        if mt==0x01:
            nc=struct.unpack(">I",s.recv(4))[0]
            vals,names=[],[]
            for _ in range(nc):
                n=struct.unpack(">I",s.recv(4))[0]; vals.append(s.recv(n).decode() if n else "")
            for _ in range(nc):
                n=struct.unpack(">I",s.recv(4))[0]; names.append(s.recv(n).decode() if n else "")
            rows.append(dict(zip(names,vals)))
        elif mt==0x04:
            rt=struct.unpack(">I",s.recv(4))[0]
            if rt==0x02: s.recv(4); return True,rows
            else:
                n=struct.unpack(">I",s.recv(4))[0]; return False,s.recv(n).decode()

conn=socket.socket(); conn.connect((HOST,PORT))
print("="*70)
print(f"PERFORMANCE EVALUATION  ({TARGET_ROWS:,} rows, batch size={BATCH_SIZE})")
print("="*70)

xact(conn,"DROP TABLE IF EXISTS PERF_TEST")
xact(conn,"CREATE TABLE PERF_TEST(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(32) NOT NULL, EMAIL VARCHAR(48) NOT NULL, BALANCE DECIMAL NOT NULL, EXPIRES_AT DECIMAL NOT NULL)")

print(f"\n── BATCH INSERT {TARGET_ROWS:,} rows (batch={BATCH_SIZE}) ──────────────────────")
t0=time.time()
inserted=0
while inserted < TARGET_ROWS:
    batch=min(BATCH_SIZE, TARGET_ROWS-inserted)
    vals=",".join(f"({inserted+i+1},'u{inserted+i+1}','u{inserted+i+1}@m.com',{1000+(inserted+i)%9999},1893456000)" for i in range(batch))
    ok,err=xact(conn,f"INSERT INTO PERF_TEST VALUES {vals}")
    if not ok: print(f"  FAIL at {inserted}: {err}"); break
    inserted+=batch
    if inserted%(TARGET_ROWS//5)==0 or inserted==TARGET_ROWS:
        elapsed=time.time()-t0
        print(f"  {inserted:>8,}/{TARGET_ROWS:,}  ({inserted/elapsed:,.0f} rows/sec)")
t1=time.time()

insert_time=t1-t0
insert_rate=TARGET_ROWS/insert_time
print(f"\n  Total INSERT time : {insert_time:.3f}s")
print(f"  Throughput        : {insert_rate:,.0f} rows/sec")

print(f"\n── SELECT * (full scan {TARGET_ROWS:,} rows) ─────────────────────────────────")
t2=time.time(); ok,rows=xact(conn,"SELECT * FROM PERF_TEST"); t3=time.time()
print(f"  Rows returned     : {len(rows):,}")
print(f"  Scan time         : {t3-t2:.3f}s  ({len(rows)/(t3-t2):,.0f} rows/sec)")

print(f"\n── SELECT WHERE ID=X (PK index, 1000 lookups) ──────────────────────────")
t4=time.time()
for i in range(1000): xact(conn,f"SELECT * FROM PERF_TEST WHERE ID = {(i%TARGET_ROWS)+1}")
t5=time.time()
print(f"  1000 lookups      : {t5-t4:.3f}s  ({1000/(t5-t4):,.0f}/sec  |  {(t5-t4)/1000*1000:.2f}ms avg)")

print(f"\n── Memory (server process) ──────────────────────────────────────────────")
try:
    import subprocess
    r=subprocess.run(["ps","aux"],capture_output=True,text=True)
    for line in r.stdout.splitlines():
        if "flexql-server" in line and "grep" not in line:
            parts=line.split(); rss=float(parts[5]) if len(parts)>5 else 0
            print(f"  Server RSS        : {rss/1024:.1f} MB")
            break
except: pass

conn.close()
print()
print("="*70)
print("SUMMARY")
print(f"  INSERT: {insert_time:.2f}s for {TARGET_ROWS:,} rows = {insert_rate:,.0f} rows/sec")
print(f"  SELECT full scan: {t3-t2:.3f}s  |  PK lookup: {(t5-t4)/1000*1000:.2f}ms avg")
print("="*70)
PYEOF
