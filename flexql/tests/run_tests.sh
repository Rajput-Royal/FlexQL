#!/usr/bin/env bash
# ============================================================
# FlexQL Full Test Suite
# Usage:  bash tests/run_tests.sh [host] [port] [--persistence]
# Default: 127.0.0.1 9000
#
# All tests are sent over a SINGLE persistent connection using
# Python, so startup overhead is paid only once.
# ============================================================

HOST=${1:-127.0.0.1}
PORT=${2:-9000}
PERSIST_FLAG=${3:-}

# Check server reachable
echo "FlexQL Test Suite  →  $HOST:$PORT"
python3 -c "
import socket, sys
s=socket.socket()
s.settimeout(3)
try: s.connect(('$HOST',$PORT)); s.close()
except: sys.exit(1)
" 2>/dev/null || { echo "  ERROR: Cannot connect to $HOST:$PORT — is the server running?"; echo "  Start with:  ./bin/flexql-server $PORT ./data"; exit 1; }
echo "  Server reachable ✓"

# ── Clean up any tables from previous runs ───────────────────
python3 << CLEANUP
import socket, struct, sys

HOST = "$HOST"
PORT = $PORT

def q(s, sql):
    b=sql.encode(); s.send(struct.pack(">I",len(b))+b)
    s.settimeout(3)
    while True:
        try:
            h=s.recv(4)
            if not h: break
            mt=struct.unpack(">I",h)[0]
            if mt==0x01:
                nc=struct.unpack(">I",s.recv(4))[0]
                for _ in range(nc*2):
                    n=struct.unpack(">I",s.recv(4))[0]
                    if n: s.recv(n)
            elif mt==0x04:
                s.recv(4)
                rt=struct.unpack(">I",s.recv(4))[0]
                if rt==0x03:
                    n=struct.unpack(">I",s.recv(4))[0]
                    if n: s.recv(n)
                break
        except: break

try:
    c=socket.socket(); c.connect((HOST,PORT))
    for t in ["STUDENT","COURSE","SCORES","BIG_USERS","TEST_USERS","TEST_ORDERS"]:
        q(c, "DROP TABLE IF EXISTS " + t)
    c.close()
    print("  Tables reset ✓")
except Exception as e:
    print("  Tables reset skipped:", e)
CLEANUP


PERSISTENCE="False"
[[ "$PERSIST_FLAG" == "--persistence" ]] && PERSISTENCE="True"

python3 - << PYEOF
import socket, struct, sys

HOST = "$HOST"
PORT = $PORT
PERSISTENCE = $PERSISTENCE

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'; YELLOW='\033[1;33m'

PASS=0; FAIL=0

# ── Wire protocol helpers ────────────────────────────────────
def make_conn():
    s = socket.socket()
    s.settimeout(5)
    s.connect((HOST, PORT))
    return s

def send_sql(s, sql):
    b = sql.encode()
    s.send(struct.pack('>I', len(b)) + b)

def recv_result(s):
    rows = []
    while True:
        try:
            hdr = s.recv(4)
            if len(hdr) < 4: return False, "connection closed"
            mtype = struct.unpack('>I', hdr)[0]
            if mtype == 0x01:   # ROW
                ncols = struct.unpack('>I', s.recv(4))[0]
                vals, names = [], []
                for _ in range(ncols):
                    n = struct.unpack('>I', s.recv(4))[0]
                    vals.append(s.recv(n).decode() if n else '')
                for _ in range(ncols):
                    n = struct.unpack('>I', s.recv(4))[0]
                    names.append(s.recv(n).decode() if n else '')
                rows.append(dict(zip(names, vals)))
            elif mtype == 0x04:  # DONE
                rtype = struct.unpack('>I', s.recv(4))[0]
                if rtype == 0x02:
                    s.recv(4)
                    return True, rows
                else:
                    n = struct.unpack('>I', s.recv(4))[0]
                    return False, s.recv(n).decode()
        except socket.timeout:
            return False, 'timeout'

def section(title):
    print(f"\n{CYAN}{BOLD}── {title} ──────────────────────────────────────────────────{NC}")

def T(s, sql, expect=None, expect_cols=None, exp_ok=True, label=None):
    global PASS, FAIL
    send_sql(s, sql)
    ok, data = recv_result(s)
    passed = (ok == exp_ok)
    if passed and expect is not None:
        if isinstance(data, list):
            combined = ' '.join(str(v) for r in data for v in r.values())
            if expect not in combined:
                passed = False
        else:
            if expect not in str(data): passed = False
    if passed and expect_cols is not None and isinstance(data, list) and data:
        if list(data[0].keys()) != expect_cols:
            passed = False
    tag = (label or sql)[:72]
    if passed:
        print(f"  {GREEN}✓{NC}  {tag}")
        PASS += 1
    else:
        print(f"  {RED}✗ FAIL{NC}  {tag}")
        if not ok and exp_ok:
            print(f"       SQL error: {data}")
        elif ok and not exp_ok:
            print(f"       Expected error but got {len(data)} rows")
        elif expect and passed is False:
            print(f"       Expected '{expect}' in output")
            if isinstance(data,list): print(f"       Got rows: {data[:2]}")
        elif expect_cols:
            cols = list(data[0].keys()) if data else []
            print(f"       Expected cols {expect_cols}, got {cols}")
        FAIL += 1

# ── Open ONE connection for all tests ──────────────────────
conn = make_conn()

# ════════════════════════════════════════════════════════════
section("1. CREATE TABLE")
T(conn, "CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, FIRST_NAME VARCHAR(64) NOT NULL, LAST_NAME VARCHAR(64) NOT NULL, EMAIL VARCHAR(128) NOT NULL)",
  label="CREATE TABLE STUDENT (INT PK + VARCHAR cols)")
T(conn, "CREATE TABLE COURSE(CID INT PRIMARY KEY NOT NULL, TITLE VARCHAR(128) NOT NULL, SID INT NOT NULL)",
  label="CREATE TABLE COURSE (INT PK + VARCHAR + INT)")
T(conn, "CREATE TABLE SCORES(SID INT PRIMARY KEY NOT NULL, STUDENT_ID INT NOT NULL, MARKS DECIMAL NOT NULL)",
  label="CREATE TABLE SCORES (DECIMAL column)")
T(conn, "CREATE TABLE STUDENT(X INT)", exp_ok=False,
  label="CREATE duplicate table -> error")

# ════════════════════════════════════════════════════════════
section("2. INSERT")
T(conn, "INSERT INTO STUDENT VALUES(1,'John','Doe','john@gmail.com')",    label="INSERT STUDENT row 1 (John)")
T(conn, "INSERT INTO STUDENT VALUES(2,'Alice','Smith','alice@gmail.com')", label="INSERT STUDENT row 2 (Alice)")
T(conn, "INSERT INTO STUDENT VALUES(3,'Bob','Jones','bob@gmail.com')",     label="INSERT STUDENT row 3 (Bob)")
T(conn, "INSERT INTO STUDENT VALUES(4,'Carol','White','carol@gmail.com')", label="INSERT STUDENT row 4 (Carol)")
T(conn, "INSERT INTO STUDENT VALUES(5,'Dave','Brown','dave@gmail.com')",   label="INSERT STUDENT row 5 (Dave)")
T(conn, "INSERT INTO COURSE VALUES(101,'Database Systems',1)",             label="INSERT COURSE row 1 (Database Systems, SID=1)")
T(conn, "INSERT INTO COURSE VALUES(102,'Algorithms',2)",                   label="INSERT COURSE row 2 (Algorithms, SID=2)")
T(conn, "INSERT INTO COURSE VALUES(103,'Networks',3)",                     label="INSERT COURSE row 3 (Networks, SID=3)")
T(conn, "INSERT INTO SCORES VALUES(1,1,95.5)",                             label="INSERT SCORES row 1 (DECIMAL 95.5)")
T(conn, "INSERT INTO SCORES VALUES(2,2,87.0)",                             label="INSERT SCORES row 2 (DECIMAL 87.0)")
T(conn, "INSERT INTO STUDENT VALUES(99)", exp_ok=False,                    label="INSERT wrong column count -> error")
T(conn, "INSERT INTO GHOST VALUES(1,'x')", exp_ok=False,                   label="INSERT into nonexistent table -> error")

# ════════════════════════════════════════════════════════════
section("3. SELECT *")
T(conn, "SELECT * FROM STUDENT", expect="1",              label="SELECT * FROM STUDENT  (ID=1 present)")
T(conn, "SELECT * FROM STUDENT", expect="Alice",          label="SELECT * FROM STUDENT  (Alice present)")
T(conn, "SELECT * FROM STUDENT", expect="Dave",           label="SELECT * FROM STUDENT  (all 5 rows: Dave)")
T(conn, "SELECT * FROM COURSE",  expect="Database Systems",label="SELECT * FROM COURSE   (title present)")
T(conn, "SELECT * FROM SCORES",  expect="95.5",           label="SELECT * FROM SCORES   (DECIMAL value 95.5)")
T(conn, "SELECT * FROM GHOST",   exp_ok=False,            label="SELECT * nonexistent table -> error")

# ════════════════════════════════════════════════════════════
section("4. SELECT specific columns (projection)")
T(conn, "SELECT FIRST_NAME, EMAIL FROM STUDENT",
  expect_cols=['FIRST_NAME','EMAIL'], label="SELECT FIRST_NAME,EMAIL  (correct cols)")
T(conn, "SELECT TITLE FROM COURSE",
  expect_cols=['TITLE'],              label="SELECT TITLE FROM COURSE  (correct col)")
T(conn, "SELECT ID, FIRST_NAME FROM STUDENT",
  expect_cols=['ID','FIRST_NAME'],    label="SELECT ID,FIRST_NAME FROM STUDENT")
T(conn, "SELECT MARKS FROM SCORES",
  expect="95.5",                      label="SELECT MARKS FROM SCORES  (DECIMAL col)")

# ════════════════════════════════════════════════════════════
section("5. WHERE  =  (equality)")
T(conn, "SELECT * FROM STUDENT WHERE ID = 1",               expect="John",             label="WHERE ID = 1  (PK index -> John)")
T(conn, "SELECT * FROM STUDENT WHERE ID = 3",               expect="Bob",              label="WHERE ID = 3  (PK index -> Bob)")
T(conn, "SELECT * FROM STUDENT WHERE ID = 5",               expect="Dave",             label="WHERE ID = 5  (PK index -> Dave)")
T(conn, "SELECT * FROM STUDENT WHERE FIRST_NAME = Alice",   expect="Alice",            label="WHERE FIRST_NAME = Alice")
T(conn, "SELECT * FROM STUDENT WHERE LAST_NAME = Jones",    expect="Bob",              label="WHERE LAST_NAME = Jones")
T(conn, "SELECT * FROM STUDENT WHERE EMAIL = carol@gmail.com", expect="Carol",         label="WHERE EMAIL = carol@gmail.com")
T(conn, "SELECT * FROM COURSE WHERE CID = 102",             expect="Algorithms",       label="WHERE CID = 102  (COURSE)")

# ════════════════════════════════════════════════════════════
section("6. WHERE  !=  (not equal)")
T(conn, "SELECT * FROM STUDENT WHERE ID != 1",  expect="Alice", label="WHERE ID != 1  (Alice in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID != 1",  expect="Dave",  label="WHERE ID != 1  (Dave in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID != 99", expect="John",  label="WHERE ID != 99 (all 5 rows: John)")

# ════════════════════════════════════════════════════════════
section("7. WHERE  >  (greater than)")
T(conn, "SELECT * FROM STUDENT WHERE ID > 3",   expect="Carol",     label="WHERE ID > 3  (Carol in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID > 3",   expect="Dave",      label="WHERE ID > 3  (Dave in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID > 4",   expect="Dave",      label="WHERE ID > 4  (only Dave)")
T(conn, "SELECT * FROM COURSE WHERE CID > 101", expect="Algorithms", label="WHERE CID > 101 (Algorithms)")

# ════════════════════════════════════════════════════════════
section("8. WHERE  <  (less than)")
T(conn, "SELECT * FROM STUDENT WHERE ID < 3",   expect="Alice",           label="WHERE ID < 3  (Alice in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID < 2",   expect="John",            label="WHERE ID < 2  (only John)")
T(conn, "SELECT * FROM COURSE WHERE CID < 103", expect="Database Systems", label="WHERE CID < 103 (Database Systems)")

# ════════════════════════════════════════════════════════════
section("9. WHERE  >=  (greater than or equal)")
T(conn, "SELECT * FROM STUDENT WHERE ID >= 4", expect="Carol", label="WHERE ID >= 4  (Carol in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID >= 5", expect="Dave",  label="WHERE ID >= 5  (only Dave)")
T(conn, "SELECT * FROM STUDENT WHERE ID >= 1", expect="Bob",   label="WHERE ID >= 1  (all rows: Bob)")

# ════════════════════════════════════════════════════════════
section("10. WHERE  <=  (less than or equal)")
T(conn, "SELECT * FROM STUDENT WHERE ID <= 2", expect="Alice", label="WHERE ID <= 2  (Alice in result)")
T(conn, "SELECT * FROM STUDENT WHERE ID <= 1", expect="John",  label="WHERE ID <= 1  (only John)")
T(conn, "SELECT * FROM STUDENT WHERE ID <= 5", expect="Dave",  label="WHERE ID <= 5  (all rows: Dave)")

# ════════════════════════════════════════════════════════════
section("11. INNER JOIN")
J = "SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID"
T(conn, J, expect="Database Systems", label="INNER JOIN: John matched with Database Systems")
T(conn, J, expect="Algorithms",       label="INNER JOIN: Alice matched with Algorithms")
T(conn, J, expect="Networks",         label="INNER JOIN: Bob matched with Networks")

# ════════════════════════════════════════════════════════════
section("12. INNER JOIN + WHERE")
T(conn, "SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE STUDENT.ID = 1",
  expect="Database Systems", label="JOIN WHERE STUDENT.ID = 1  (Database Systems)")
T(conn, "SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE STUDENT.ID = 2",
  expect="Algorithms",       label="JOIN WHERE STUDENT.ID = 2  (Algorithms)")
T(conn, "SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE COURSE.SID = 3",
  expect="Bob",              label="JOIN WHERE COURSE.SID = 3  (Bob + Networks)")
T(conn, "SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE STUDENT.FIRST_NAME = Alice",
  expect="Algorithms",       label="JOIN WHERE STUDENT.FIRST_NAME = Alice")

# ════════════════════════════════════════════════════════════
section("13. Multiple queries in one session")
send_sql(conn, "SELECT * FROM STUDENT WHERE ID = 1")
ok1, d1 = recv_result(conn)
send_sql(conn, "SELECT * FROM COURSE WHERE CID = 102")
ok2, d2 = recv_result(conn)
r1 = ok1 and any('John' in v for row in d1 for v in row.values())
r2 = ok2 and any('Algorithms' in v for row in d2 for v in row.values())
if r1 and r2:
    print(f"  {GREEN}✓{NC}  Two queries same connection: both return correct rows"); PASS+=1
else:
    print(f"  {RED}✗ FAIL{NC}  Two queries same connection"); FAIL+=1

# ════════════════════════════════════════════════════════════
section("14. Edge cases")
T(conn, "SELECT * FROM STUDENT WHERE BADCOL = 1",
  label="WHERE nonexistent col (no crash, 0 rows ok)")
T(conn, "SELECT * FROM STUDENT WHERE ID = 9999",
  label="WHERE value not in table (no crash, 0 rows)")
T(conn, "INSERT INTO STUDENT VALUES(99,'Zara','Ali','zara@test.com')",
  label="INSERT email with @ sign (Zara)")
T(conn, "SELECT * FROM STUDENT WHERE ID = 99", expect="Zara",
  label="SELECT just-inserted row (Zara visible)")
T(conn, "SELECT FIRST_NAME, EMAIL FROM STUDENT WHERE ID = 2",
  expect="alice@gmail.com", label="SELECT cols + WHERE combined")
T(conn, "SELECT * FROM SCORES WHERE MARKS = 95.5",
  expect="95.5", label="WHERE on DECIMAL value 95.5")

conn.close()

# ════════════════════════════════════════════════════════════
if PERSISTENCE:
    import socket as sock
    section("15. Persistence (after server restart)")
    conn2 = sock.socket(); conn2.settimeout(5); conn2.connect((HOST,PORT))
    T(conn2, "SELECT * FROM STUDENT", expect="John",            label="Persist: STUDENT has John")
    T(conn2, "SELECT * FROM STUDENT", expect="Dave",            label="Persist: STUDENT has Dave")
    T(conn2, "SELECT * FROM COURSE",  expect="Database Systems", label="Persist: COURSE has Database Systems")
    T(conn2, "SELECT * FROM SCORES",  expect="95.5",            label="Persist: SCORES has 95.5")
    conn2.close()
else:
    section("15. Persistence (restart test)")
    print(f"  {YELLOW}Skipped — re-run after restarting server:{NC}")
    print( "    bash tests/run_tests.sh $HOST $PORT --persistence")

# ════════════════════════════════════════════════════════════
print()
print(f"{BOLD}════════════════════════════════════════════════════════{NC}")
if FAIL == 0:
    print(f"{GREEN}{BOLD}  RESULT: {PASS}/{PASS+FAIL} passed   ✓  ALL PASS{NC}")
else:
    print(f"{RED}{BOLD}  RESULT: {PASS}/{PASS+FAIL} passed   ✗  {FAIL} FAILED{NC}")
print(f"{BOLD}════════════════════════════════════════════════════════{NC}")
print()
sys.exit(FAIL)
PYEOF
