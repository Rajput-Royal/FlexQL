import socket, struct, os, time, threading, subprocess, sys

PORT = 19432
PASS = 0; FAIL = 0

def xact(s, sql):
    b = sql.encode(); s.send(struct.pack('>I', len(b)) + b); s.settimeout(3.0); rows = []
    while True:
        try:
            raw = s.recv(4)
            if len(raw) < 4: return False, "closed"
            mtype = struct.unpack('>I', raw)[0]
            if mtype == 0x01:
                ncols = struct.unpack('>I', s.recv(4))[0]; vals, names = [], []
                for _ in range(ncols):
                    n = struct.unpack('>I', s.recv(4))[0]; vals.append(s.recv(n).decode() if n else '')
                for _ in range(ncols):
                    n = struct.unpack('>I', s.recv(4))[0]; names.append(s.recv(n).decode() if n else '')
                rows.append(dict(zip(names, vals)))
            elif mtype == 0x04:
                rtype = struct.unpack('>I', s.recv(4))[0]
                if rtype == 0x02: s.recv(4); return True, rows
                else:
                    n = struct.unpack('>I', s.recv(4))[0]; return False, s.recv(n).decode()
        except Exception as e: return False, str(e)

def T(s, label, sql, exp_ok=True, exp_rows=None, exp_cols=None, exp_vals=None):
    global PASS, FAIL
    ok, data = xact(s, sql); p = (ok == exp_ok)
    if p and exp_rows is not None: p = isinstance(data, list) and len(data) == exp_rows
    if p and exp_cols is not None: p = isinstance(data, list) and data and list(data[0].keys()) == exp_cols
    if p and exp_vals is not None: p = isinstance(data, list) and data and data[0] == exp_vals
    icon = "✓" if p else "✗"; st = "PASS" if p else "FAIL"
    ri = f"{len(data)} rows" if isinstance(data, list) else f"err={str(data)[:50]}"
    print(f"  {icon} [{st}] {label}")
    if not p:
        print(f"         sql: {sql[:75]}"); print(f"         got: {ri}")
        if exp_rows is not None: print(f"         exp_rows={exp_rows}")
        if exp_vals is not None: print(f"         exp_vals={exp_vals}")
    if p: PASS += 1
    else: FAIL += 1

def H(t): print(f"\n{'='*65}\n  {t}\n{'='*65}")

s = socket.socket(); s.connect(('127.0.0.1', PORT))

H("1. CREATE TABLE")
T(s,"1.1 STUDENT","CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, FIRST_NAME VARCHAR(64) NOT NULL, LAST_NAME VARCHAR(64) NOT NULL, EMAIL VARCHAR(128) NOT NULL)")
T(s,"1.2 COURSE","CREATE TABLE COURSE(CID INT PRIMARY KEY NOT NULL, TITLE VARCHAR(128) NOT NULL, SID INT NOT NULL)")
T(s,"1.3 SCORES (DECIMAL)","CREATE TABLE SCORES(SID INT PRIMARY KEY NOT NULL, STUDENT_ID INT NOT NULL, MARKS DECIMAL NOT NULL)")
T(s,"1.4 NUMS","CREATE TABLE NUMS(A INT PRIMARY KEY NOT NULL, B INT NOT NULL, C INT NOT NULL)")
T(s,"1.5 Duplicate table -> error","CREATE TABLE STUDENT(X INT)",exp_ok=False)

H("2. INSERT")
rows_s = [("1","John","Doe","john@gmail.com"),("2","Alice","Smith","alice@gmail.com"),
          ("3","Bob","Jones","bob@gmail.com"),("4","Carol","White","carol@gmail.com"),
          ("5","Dave","Black","dave@gmail.com")]
for r in rows_s:
    T(s,f"2.{r[0]} STUDENT row {r[0]}",f"INSERT INTO STUDENT VALUES({r[0]},'{r[1]}','{r[2]}','{r[3]}')")
for r in [("101","Database Systems","1"),("102","Algorithms","2"),("103","Networks","3"),("104","OS","4")]:
    T(s,f"2.c COURSE {r[0]}",f"INSERT INTO COURSE VALUES({r[0]},'{r[1]}',{r[2]})")
T(s,"2.d SCORES 95.5",  "INSERT INTO SCORES VALUES(1,1,95.5)")
T(s,"2.e SCORES 88.0",  "INSERT INTO SCORES VALUES(2,2,88.0)")
T(s,"2.f SCORES 72.3",  "INSERT INTO SCORES VALUES(3,3,72.3)")
T(s,"2.g NUMS 10,20,30","INSERT INTO NUMS VALUES(10,20,30)")
T(s,"2.h NUMS 20,40,60","INSERT INTO NUMS VALUES(20,40,60)")
T(s,"2.i NUMS 30,60,90","INSERT INTO NUMS VALUES(30,60,90)")
T(s,"2.j wrong col count -> error","INSERT INTO STUDENT VALUES(99)",exp_ok=False)
T(s,"2.k nonexistent table -> error","INSERT INTO GHOST VALUES(1)",exp_ok=False)

H("3. SELECT *")
T(s,"3.1 STUDENT 5 rows","SELECT * FROM STUDENT",exp_rows=5,exp_cols=['ID','FIRST_NAME','LAST_NAME','EMAIL'])
T(s,"3.2 COURSE 4 rows","SELECT * FROM COURSE",exp_rows=4,exp_cols=['CID','TITLE','SID'])
T(s,"3.3 SCORES 3 rows","SELECT * FROM SCORES",exp_rows=3,exp_cols=['SID','STUDENT_ID','MARKS'])
T(s,"3.4 NUMS 3 rows","SELECT * FROM NUMS",exp_rows=3)
T(s,"3.5 nonexistent -> error","SELECT * FROM GHOST",exp_ok=False)

H("4. SELECT specific columns (projection)")
T(s,"4.1 FIRST_NAME,EMAIL -> 2 cols","SELECT FIRST_NAME, EMAIL FROM STUDENT",exp_rows=5,exp_cols=['FIRST_NAME','EMAIL'])
T(s,"4.2 TITLE only","SELECT TITLE FROM COURSE",exp_rows=4,exp_cols=['TITLE'])
T(s,"4.3 ID,FIRST_NAME","SELECT ID, FIRST_NAME FROM STUDENT",exp_rows=5,exp_cols=['ID','FIRST_NAME'])
T(s,"4.4 MARKS only","SELECT MARKS FROM SCORES",exp_rows=3,exp_cols=['MARKS'])
T(s,"4.5 B from NUMS","SELECT B FROM NUMS",exp_rows=3,exp_cols=['B'])
T(s,"4.6 reverse col order","SELECT EMAIL, LAST_NAME, ID FROM STUDENT",exp_rows=5,exp_cols=['EMAIL','LAST_NAME','ID'])

H("5. WHERE operator =")
T(s,"5.1 ID=1 (PK index)","SELECT * FROM STUDENT WHERE ID = 1",exp_rows=1,exp_vals={'ID':'1','FIRST_NAME':'John','LAST_NAME':'Doe','EMAIL':'john@gmail.com'})
T(s,"5.2 ID=3","SELECT * FROM STUDENT WHERE ID = 3",exp_rows=1,exp_vals={'ID':'3','FIRST_NAME':'Bob','LAST_NAME':'Jones','EMAIL':'bob@gmail.com'})
T(s,"5.3 ID=999 -> 0 rows","SELECT * FROM STUDENT WHERE ID = 999",exp_rows=0)
T(s,"5.4 VARCHAR=Alice","SELECT * FROM STUDENT WHERE FIRST_NAME = Alice",exp_rows=1)
T(s,"5.5 VARCHAR=Jones","SELECT * FROM STUDENT WHERE LAST_NAME = Jones",exp_rows=1)
T(s,"5.6 non-PK INT=2","SELECT * FROM COURSE WHERE SID = 2",exp_rows=1)

H("6. WHERE operator >")
T(s,"6.1 ID>3 -> 2","SELECT * FROM STUDENT WHERE ID > 3",exp_rows=2)
T(s,"6.2 ID>1 -> 4","SELECT * FROM STUDENT WHERE ID > 1",exp_rows=4)
T(s,"6.3 ID>5 -> 0","SELECT * FROM STUDENT WHERE ID > 5",exp_rows=0)
T(s,"6.4 ID>0 -> 5","SELECT * FROM STUDENT WHERE ID > 0",exp_rows=5)

H("7. WHERE operator <")
T(s,"7.1 ID<3 -> 2","SELECT * FROM STUDENT WHERE ID < 3",exp_rows=2)
T(s,"7.2 ID<1 -> 0","SELECT * FROM STUDENT WHERE ID < 1",exp_rows=0)
T(s,"7.3 ID<6 -> 5","SELECT * FROM STUDENT WHERE ID < 6",exp_rows=5)

H("8. WHERE operator >=")
T(s,"8.1 ID>=3 -> 3","SELECT * FROM STUDENT WHERE ID >= 3",exp_rows=3)
T(s,"8.2 ID>=1 -> 5","SELECT * FROM STUDENT WHERE ID >= 1",exp_rows=5)
T(s,"8.3 ID>=5 -> 1","SELECT * FROM STUDENT WHERE ID >= 5",exp_rows=1)
T(s,"8.4 ID>=6 -> 0","SELECT * FROM STUDENT WHERE ID >= 6",exp_rows=0)

H("9. WHERE operator <=")
T(s,"9.1 ID<=3 -> 3","SELECT * FROM STUDENT WHERE ID <= 3",exp_rows=3)
T(s,"9.2 ID<=1 -> 1","SELECT * FROM STUDENT WHERE ID <= 1",exp_rows=1)
T(s,"9.3 ID<=5 -> 5","SELECT * FROM STUDENT WHERE ID <= 5",exp_rows=5)
T(s,"9.4 ID<=0 -> 0","SELECT * FROM STUDENT WHERE ID <= 0",exp_rows=0)

H("10. WHERE operator !=")
T(s,"10.1 ID!=1 -> 4","SELECT * FROM STUDENT WHERE ID != 1",exp_rows=4)
T(s,"10.2 ID!=3 -> 4","SELECT * FROM STUDENT WHERE ID != 3",exp_rows=4)
T(s,"10.3 ID!=999 -> 5","SELECT * FROM STUDENT WHERE ID != 999",exp_rows=5)
T(s,"10.4 FIRST_NAME!=Alice -> 4","SELECT * FROM STUDENT WHERE FIRST_NAME != Alice",exp_rows=4)

H("11. WHERE + projection combined")
T(s,"11.1 SELECT FIRST_NAME WHERE ID=2","SELECT FIRST_NAME FROM STUDENT WHERE ID = 2",exp_rows=1,exp_cols=['FIRST_NAME'],exp_vals={'FIRST_NAME':'Alice'})
T(s,"11.2 SELECT EMAIL WHERE LAST_NAME=Jones","SELECT EMAIL FROM STUDENT WHERE LAST_NAME = Jones",exp_rows=1,exp_cols=['EMAIL'],exp_vals={'EMAIL':'bob@gmail.com'})
T(s,"11.3 SELECT MARKS WHERE STUDENT_ID=1","SELECT MARKS FROM SCORES WHERE STUDENT_ID = 1",exp_rows=1,exp_cols=['MARKS'],exp_vals={'MARKS':'95.5'})

H("12. DECIMAL type")
T(s,"12.1 DECIMAL stored/retrieved","SELECT * FROM SCORES WHERE SID = 1",exp_rows=1,exp_vals={'SID':'1','STUDENT_ID':'1','MARKS':'95.5'})
T(s,"12.2 MARKS>80 -> 2","SELECT * FROM SCORES WHERE MARKS > 80",exp_rows=2)
T(s,"12.3 MARKS<90 -> 2","SELECT * FROM SCORES WHERE MARKS < 90",exp_rows=2)
T(s,"12.4 MARKS>=88 -> 2","SELECT * FROM SCORES WHERE MARKS >= 88",exp_rows=2)
T(s,"12.5 MARKS<=88 -> 2","SELECT * FROM SCORES WHERE MARKS <= 88",exp_rows=2)
T(s,"12.6 MARKS!=95.5 -> 2","SELECT * FROM SCORES WHERE MARKS != 95.5",exp_rows=2)

H("13. INNER JOIN")
T(s,"13.1 JOIN 4 rows","SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID",exp_rows=4)
T(s,"13.2 JOIN 7 cols","SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID",exp_cols=['ID','FIRST_NAME','LAST_NAME','EMAIL','CID','TITLE','SID'])
T(s,"13.3 JOIN+WHERE ID=1","SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE STUDENT.ID = 1",exp_rows=1)
T(s,"13.4 JOIN+WHERE SID=2","SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE COURSE.SID = 2",exp_rows=1)
T(s,"13.5 JOIN no match -> 0","SELECT * FROM STUDENT INNER JOIN COURSE ON STUDENT.ID = COURSE.SID WHERE STUDENT.ID = 5",exp_rows=0)
T(s,"13.6 JOIN STUDENT x SCORES -> 3","SELECT * FROM STUDENT INNER JOIN SCORES ON STUDENT.ID = SCORES.STUDENT_ID",exp_rows=3)
T(s,"13.7 JOIN nonexistent -> error","SELECT * FROM STUDENT INNER JOIN GHOST ON STUDENT.ID = GHOST.ID",exp_ok=False)

H("14. Persistence")
T(s,"14.1 insert marker","INSERT INTO STUDENT VALUES(99,'Persist','Check','p@test.com')")
s.close()
os.system("pkill -9 -f flexql-server 2>/dev/null"); time.sleep(0.5)
subprocess.Popen(["./bin/flexql-server","19432","./data"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
time.sleep(0.7)
s2 = socket.socket(); s2.connect(('127.0.0.1', 19432))
T(s2,"14.2 STUDENT 6 rows after restart","SELECT * FROM STUDENT",exp_rows=6)
T(s2,"14.3 marker row ID=99 present","SELECT * FROM STUDENT WHERE ID = 99",exp_rows=1,exp_vals={'ID':'99','FIRST_NAME':'Persist','LAST_NAME':'Check','EMAIL':'p@test.com'})
T(s2,"14.4 COURSE 4 rows","SELECT * FROM COURSE",exp_rows=4)
T(s2,"14.5 SCORES 3 rows","SELECT * FROM SCORES",exp_rows=3)
T(s2,"14.6 NUMS 3 rows","SELECT * FROM NUMS",exp_rows=3)

H("15. Multi-client concurrency")
results = {}
def cq(cid):
    try:
        sc = socket.socket(); sc.connect(('127.0.0.1', 19432))
        ok, data = xact(sc, "SELECT * FROM STUDENT")
        results[cid] = (ok, len(data) if isinstance(data,list) else 0); sc.close()
    except: results[cid] = (False, 0)
threads = [threading.Thread(target=cq, args=(i,)) for i in range(5)]
for t in threads: t.start()
for t in threads: t.join()
all_ok = all(v[0] and v[1]==6 for v in results.values())
icon, st = ("✓","PASS") if all_ok else ("✗","FAIL")
print(f"  {icon} [{st}] 15.1 5 concurrent clients all get 6 rows")
if not all_ok: print(f"         results={results}")
if all_ok: PASS += 1
else: FAIL += 1

s2.close(); os.system("pkill -9 -f flexql-server 2>/dev/null")
print(f"\n{'='*65}")
print(f"  TOTAL: {PASS}/{PASS+FAIL} tests passed  {'ALL PASS' if FAIL==0 else f'{FAIL} FAILED'}")
print(f"{'='*65}")
