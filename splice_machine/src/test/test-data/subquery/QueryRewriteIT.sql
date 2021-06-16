-- Make sure predicate simplification is not turned off.

create table T1(a1 int, b1 int, c1 int);
create table T2(a2 int, b2 int, c2 int);
create table T3(a3 int, b3 int, c3 int);

-- values that exist in all tables
INSERT INTO T1 VALUES (0, 0, 0),(1, 10, 100);
INSERT INTO T2 VALUES (0, 0, 0),(1, 10, 100);
INSERT INTO T3 VALUES (0, 0, 0),(1, 10, 100);

-- combinations
INSERT INTO T1 VALUES (2, 20, 200), (3, 30, 300), (4, 40, 400), (5, 50, 500), (6, 60, 600), (7, 70, 700);
INSERT INTO T2 VALUES (2, 20, 200), (3, 30, 300),               (5, 50, 500), (6, 60, 600)                       ,(8, 80, 800), (9, 90, 900);
INSERT INTO T3 VALUES (2, 20, 200),                             (5, 50, 500)                           ,(8, 80, 800)             , (10, 100, 1000);

-- duplicates
INSERT INTO T1 VALUES (11, 110, 1100);
INSERT INTO T2 VALUES (11, 110, 1100), (11, 110, 1100);
INSERT INTO T3 VALUES (11, 110, 1100), (11, 110, 1100), (11, 110, 1100);
INSERT INTO T1 VALUES (12, 120, 1200), (12, 120, 1200);

-- nulls
INSERT INTO T1 VALUES (NULL, NULL, NULL);
INSERT INTO T2 VALUES (NULL, NULL, NULL), (NULL, NULL, NULL);
INSERT INTO T3 VALUES (NULL, NULL, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL);

-- values that only exist in T1 
INSERT INTO T1 VALUES (13, 0, 0), (13, 1, 1);

