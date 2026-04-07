#!/usr/bin/env bash
# ============================================================
# Q1: Test all supported data types: INT, DECIMAL, VARCHAR
# DATETIME is parsed but stored as DECIMAL (per spec)
# Usage: bash tests/test_datatypes.sh [host] [port]
# ============================================================
HOST=${1:-127.0.0.1}; PORT=${2:-9000}

python3 << PYEOF
import socket, struct, sys

HOST="$HOST"; PORT=$PORT

def xact(s, sql):
    b=sql.encode(); s.send(struct.pack(">I",len(b))+b)
    s.settimeout(5); rows=[]
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

P,F=0,0
def T(s,sql,exp=None,exp_ok=True,label=None):
    global P,F
    ok,data=xact(s,sql)
    passed=(ok==exp_ok)
    if passed and exp and isinstance(data,list):
        combined=" ".join(v for r in data for v in r.values())
        passed=exp in combined
    icon="✓" if passed else "✗ FAIL"
    tag=(label or sql)[:70]
    info=f"{len(data)} rows  {data[0] if data else ''}" if isinstance(data,list) else str(data)[:60]
    print(f"  {icon}  {tag:70s}  {info}")
    if passed: P+=1
    else: F+=1

conn=socket.socket(); conn.connect((HOST,PORT))
print("="*80)
print("DATA TYPE VERIFICATION SCRIPT")
print("="*80)

print("\n── Setup: Drop existing tables ─────────────────────────────────────────────")
for t in ["DT_TEST"]: xact(conn,"DROP TABLE IF EXISTS "+t)

print("\n── 1. INT type ─────────────────────────────────────────────────────────────")
T(conn,"CREATE TABLE DT_TEST(ID INT PRIMARY KEY NOT NULL, VAL_INT INT NOT NULL, VAL_DEC DECIMAL NOT NULL, VAL_STR VARCHAR(64) NOT NULL)",
  label="CREATE TABLE with INT, DECIMAL, VARCHAR")
T(conn,"INSERT INTO DT_TEST VALUES(1, 42, 3.14, 'hello world')",
  label="INSERT INT=42, DECIMAL=3.14, VARCHAR='hello world'")
T(conn,"INSERT INTO DT_TEST VALUES(2, -100, 99.99, 'test@email.com')",
  label="INSERT negative INT, decimal, email VARCHAR")
T(conn,"INSERT INTO DT_TEST VALUES(3, 2147483647, 0.001, 'max int test')",
  label="INSERT large INT=2147483647")

print("\n── 2. INT queries ───────────────────────────────────────────────────────────")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_INT = 42",      exp="42",    label="WHERE INT = 42")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_INT > 0",       exp="42",    label="WHERE INT > 0 (2 rows)")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_INT < 0",       exp="-100",  label="WHERE INT < 0 (negative)")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_INT >= 42",     exp="42",    label="WHERE INT >= 42")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_INT != 42",     exp="-100",  label="WHERE INT != 42")

print("\n── 3. DECIMAL type ──────────────────────────────────────────────────────────")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_DEC = 3.14",   exp="3.14",  label="WHERE DECIMAL = 3.14")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_DEC > 1.0",    exp="3.14",  label="WHERE DECIMAL > 1.0")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_DEC < 1.0",    exp="0.001", label="WHERE DECIMAL < 1.0")
T(conn,"SELECT VAL_DEC FROM DT_TEST",                  exp="3.14",  label="SELECT DECIMAL column")

print("\n── 4. VARCHAR type ──────────────────────────────────────────────────────────")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_STR = hello world",    exp="42",    label="WHERE VARCHAR = 'hello world'")
T(conn,"SELECT * FROM DT_TEST WHERE VAL_STR = test@email.com", exp="-100",  label="WHERE VARCHAR = email address")
T(conn,"SELECT VAL_STR FROM DT_TEST",                          exp="hello", label="SELECT VARCHAR column")

print("\n── 5. DATETIME (stored as DECIMAL — per spec) ───────────────────────────────")
xact(conn,"DROP TABLE IF EXISTS DT_TIME")
T(conn,"CREATE TABLE DT_TIME(ID INT PRIMARY KEY NOT NULL, TS DECIMAL NOT NULL)",
  label="CREATE TABLE with DATETIME-style DECIMAL column")
T(conn,"INSERT INTO DT_TIME VALUES(1, 1893456000)",
  label="INSERT Unix timestamp 1893456000")
T(conn,"SELECT * FROM DT_TIME WHERE TS = 1893456000",
  exp="1893456000", label="WHERE TS = 1893456000 (no scientific notation)")

conn.close()
print()
print("="*80)
print(f"  RESULT: {P}/{P+F} passed  {'✓ ALL PASS' if F==0 else f'✗ {F} FAILED'}")
print("="*80)
sys.exit(F)
PYEOF
