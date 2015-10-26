create table EMPTY_TABLE(e1 int, e2 int, e3 int, e4 int);

create table A(a1 int, a2 int);
create table B(b1 int, b2 int);
create table C(c1 int, c2 int);
create table D(d1 int, d2 int);

-- values that exist in all tables
INSERT INTO A VALUES (0, 0),(1, 10);
INSERT INTO B VALUES (0, 0),(1, 10);
INSERT INTO C VALUES (0, 0),(1, 10);
INSERT INTO D VALUES (0, 0),(1, 10);

-- combinations
INSERT INTO A VALUES (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70);
INSERT INTO B VALUES (2, 20), (3, 30),                   (6, 60)         ,(8, 80), (9, 90);
INSERT INTO C VALUES (2, 20),                   (5, 50)                  ,(8, 80)         , (10, 100);
INSERT INTO D VALUES                            (5, 50), (6, 60), (7, 70);

-- duplicates
INSERT INTO A VALUES (11, 110);
INSERT INTO B VALUES (11, 110), (11, 110);
INSERT INTO C VALUES (11, 110), (11, 110), (11, 110);
INSERT INTO D VALUES (11, 110), (11, 110), (11, 110), (11, 110);

-- duplicates (in A)
INSERT INTO A VALUES (12, 120), (12, 120);
INSERT INTO D VALUES (12, 120), (12, 120);

-- nulls
INSERT INTO A VALUES (NULL, NULL);
INSERT INTO B VALUES (NULL, NULL), (NULL, NULL);
INSERT INTO C VALUES (NULL, NULL), (NULL, NULL), (NULL, NULL);
INSERT INTO D VALUES (NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL);

-- values that only exist in A
INSERT INTO A VALUES (13, 0), (13, 1);

