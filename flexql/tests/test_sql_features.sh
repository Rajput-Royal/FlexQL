#!/usr/bin/env bash
# ============================================================
# Q2: Test SELECT *, SELECT cols, WHERE, INNER JOIN
# Usage: bash tests/test_sql_features.sh [host] [port]
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
def T(s,sql,exp=None,exp_rows=None,exp_cols=None,exp_ok=True,label=None):
    global P,F
    ok,data=xact(s,sql)
    passed=(ok==exp_ok)
    if passed and exp and isinstance(data,list):
        combined=" ".join(v for r in data for v in r.values())
        passed=exp in combined
    if passed and exp_rows is not None and isinstance(data,list): passed=(len(data)==exp_rows)
    if passed and exp_cols is not None and isinstance(data,list) and data: passed=(list(data[0].keys())==exp_cols)
    icon="✓" if passed else "✗ FAIL"
    tag=(label or sql)[:70]
    info=(f"{len(data)} rows" + (f"  cols={list(data[0].keys())}" if data else "")) if isinstance(data,list) else str(data)[:60]
    print(f"  {icon}  {tag:70s}  {info}")
    if passed: P+=1
    else: F+=1

conn=socket.socket(); conn.connect((HOST,PORT))
print("="*80)
print("SQL FEATURES VERIFICATION SCRIPT")
print("="*80)

print("\n── Setup ────────────────────────────────────────────────────────────────────")
for t in ["EMPLOYEES","DEPARTMENTS"]: xact(conn,"DROP TABLE IF EXISTS "+t)
T(conn,"CREATE TABLE EMPLOYEES(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(64) NOT NULL, DEPT_ID INT NOT NULL, SALARY DECIMAL NOT NULL)",
  label="CREATE TABLE EMPLOYEES")
T(conn,"CREATE TABLE DEPARTMENTS(DID INT PRIMARY KEY NOT NULL, DNAME VARCHAR(64) NOT NULL)",
  label="CREATE TABLE DEPARTMENTS")
for row in [("1","Alice","10","75000"),("2","Bob","20","50000"),("3","Carol","10","90000"),("4","Dave","30","45000")]:
    T(conn,f"INSERT INTO EMPLOYEES VALUES({row[0]},'{row[1]}',{row[2]},{row[3]})", label=f"INSERT EMPLOYEES {row[1]}")
for row in [("10","Engineering"),("20","Marketing"),("30","Finance")]:
    T(conn,f"INSERT INTO DEPARTMENTS VALUES({row[0]},'{row[1]}')", label=f"INSERT DEPT {row[1]}")

print("\n── A. SELECT ALL COLUMNS (SELECT *) ─────────────────────────────────────────")
T(conn,"SELECT * FROM EMPLOYEES",
  exp_rows=4, exp_cols=["ID","NAME","DEPT_ID","SALARY"],
  label="SELECT * FROM EMPLOYEES (4 rows, all 4 cols)")
T(conn,"SELECT * FROM DEPARTMENTS",
  exp_rows=3, exp_cols=["DID","DNAME"],
  label="SELECT * FROM DEPARTMENTS (3 rows, all 2 cols)")

print("\n── B. SELECT SPECIFIC COLUMNS ───────────────────────────────────────────────")
T(conn,"SELECT NAME, SALARY FROM EMPLOYEES",
  exp_rows=4, exp_cols=["NAME","SALARY"],
  label="SELECT NAME,SALARY (only 2 cols returned)")
T(conn,"SELECT DNAME FROM DEPARTMENTS",
  exp_rows=3, exp_cols=["DNAME"],
  label="SELECT DNAME only (1 col)")
T(conn,"SELECT ID, NAME FROM EMPLOYEES",
  exp_rows=4, exp_cols=["ID","NAME"],
  label="SELECT ID,NAME (2 cols)")

print("\n── C. WHERE clause - single condition ───────────────────────────────────────")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID = 1",     exp_rows=1, exp="Alice",  label="WHERE ID = 1  (PK index)")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID > 2",     exp_rows=2, exp="Carol",  label="WHERE ID > 2")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID < 3",     exp_rows=2, exp="Bob",    label="WHERE ID < 3")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID >= 3",    exp_rows=2, exp="Carol",  label="WHERE ID >= 3")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID <= 2",    exp_rows=2, exp="Alice",  label="WHERE ID <= 2")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID != 2",    exp_rows=3, exp="Alice",  label="WHERE ID != 2")
T(conn,"SELECT * FROM EMPLOYEES WHERE NAME = Alice",  exp_rows=1, exp="Alice", label="WHERE VARCHAR col = Alice")
T(conn,"SELECT * FROM EMPLOYEES WHERE SALARY > 60000",exp_rows=2, exp="Alice", label="WHERE DECIMAL > 60000")
T(conn,"SELECT NAME FROM EMPLOYEES WHERE DEPT_ID = 10",exp_rows=2,exp="Alice", label="WHERE DEPT_ID = 10 (2 employees)")

print("\n── D. WHERE with AND/OR should FAIL (not supported) ─────────────────────────")
T(conn,"SELECT * FROM EMPLOYEES WHERE ID > 1 AND ID < 4",
  exp_ok=False, label="WHERE ... AND ... → must return error (not supported)")

print("\n── E. INNER JOIN between two tables ─────────────────────────────────────────")
J="SELECT * FROM EMPLOYEES INNER JOIN DEPARTMENTS ON EMPLOYEES.DEPT_ID = DEPARTMENTS.DID"
T(conn,J, exp_rows=4, exp="Engineering", label="INNER JOIN EMPLOYEES-DEPARTMENTS (4 rows)")
T(conn,J+" WHERE EMPLOYEES.DEPT_ID = 10",
  exp_rows=2, exp="Engineering", label="INNER JOIN + WHERE DEPT_ID = 10")
T(conn,J+" WHERE DEPARTMENTS.DNAME = Finance",
  exp_rows=1, exp="Dave",        label="INNER JOIN + WHERE dept name = Finance")
T(conn,"SELECT EMPLOYEES.NAME, DEPARTMENTS.DNAME FROM EMPLOYEES INNER JOIN DEPARTMENTS ON EMPLOYEES.DEPT_ID = DEPARTMENTS.DID",
  exp_rows=4, exp_cols=["NAME","DNAME"], label="JOIN with specific cols: NAME, DNAME")

conn.close()
print()
print("="*80)
print(f"  RESULT: {P}/{P+F} passed  {'✓ ALL PASS' if F==0 else f'✗ {F} FAILED'}")
print("="*80)
sys.exit(F)
PYEOF
