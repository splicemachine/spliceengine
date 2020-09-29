CREATE TABLE PROJ (
  PNUM   VARCHAR(3) NOT NULL,
  PNAME  VARCHAR(20),
  PTYPE  CHAR(6),
  BUDGET DECIMAL(9),
  CITY   VARCHAR(15)
);

CREATE TABLE staff (
  EMPNUM  VARCHAR(3) NOT NULL,
  EMPNAME VARCHAR(20),
  GRADE   DECIMAL(4),
  CITY    VARCHAR(15)
);

create table t1 (
  a1 int,
  b1 int
);

create table t2 (
  a2 int,
  b2 int
);

INSERT INTO PROJ VALUES ('P1', 'MXSS', 'Design', 10000, 'Deale');
INSERT INTO PROJ VALUES ('P2', 'CALM', 'Code', 30000, 'Vienna');
INSERT INTO PROJ VALUES ('P3', 'SDP', 'Test', 30000, 'Tampa');
INSERT INTO PROJ VALUES ('P4', 'SDP', 'Design', 20000, 'Deale');
INSERT INTO PROJ VALUES ('P5', 'IRM', 'Test', 10000, 'Vienna');
INSERT INTO PROJ VALUES ('P6', 'PAYR', 'Design', 50000, 'Deale');
INSERT INTO PROJ VALUES ('P7', 'KMA', 'Design', 50000, 'Akron');


INSERT INTO STAFF VALUES ('E1', 'Alice', 12, 'Deale');
INSERT INTO STAFF VALUES ('E2', 'Betty', 10, 'Vienna');
INSERT INTO STAFF VALUES ('E3', 'Carmen', 13, 'Vienna');
INSERT INTO STAFF VALUES ('E4', 'Don', 12, 'Deale');
INSERT INTO STAFF VALUES ('E5', 'Ed', 13, 'Akron');
INSERT INTO STAFF VALUES ('E6', 'Joe', 13, 'Deale');
INSERT INTO STAFF VALUES ('E7', 'Fred', 13, 'Vienna');
INSERT INTO STAFF VALUES ('E8', 'Jane', 13, 'Akron');

insert into t1 values (1,1), (2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,1);
insert into t1 select a1+10, b1 from t1;
insert into t1 select a1+20, b1 from t1;
insert into t1 select a1+40, b1 from t1;
insert into t1 select a1+80, b1 from t1;
insert into t1 select a1+160, b1 from t1;
insert into t1 select a1+320, b1 from t1;

insert into t2 values (1,1);