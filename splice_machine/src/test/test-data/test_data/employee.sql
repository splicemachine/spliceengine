drop table if exists works;
CREATE TABLE WORKS (EMPNUM VARCHAR(3) NOT NULL,PNUM VARCHAR(3) NOT NULL,HOURS DECIMAL(5));
INSERT INTO WORKS VALUES  ('E1','P1',40) ,('E1','P2',20) ,('E1','P3',80) ,('E1','P4',20) ,('E1','P5',12)
,('E1','P6',12) ,('E2','P1',40) ,('E2','P2',80) ,('E3','P2',20) ,('E4','P2',20) ,('E4','P4',40) ,('E4','P5',80);

drop table if exists staff;
CREATE TABLE STAFF (EMPNUM VARCHAR(3) NOT NULL, EMPNAME VARCHAR(20), GRADE DECIMAL(4), CITY VARCHAR(15));
INSERT INTO STAFF VALUES ('E1','Alice',12,'Deale') , ('E2','Betty',10,'Vienna') , ('E3','Carmen',13,'Vienna')
,('E4','Don',12,'Deale'),('E5','Ed',13,'Akron');

drop table if exists upuniq;
CREATE TABLE UPUNIQ (NUMKEY INTEGER NOT NULL, COL2 VARCHAR(2));
INSERT INTO UPUNIQ VALUES(1,'A') ,(2,'B') ,(3,'C') ,(4,'D') ,(6,'F'),(8,'H');

drop table if exists proj;
CREATE TABLE PROJ (PNUM VARCHAR(3) NOT NULL, PNAME VARCHAR(20), PTYPE VARCHAR(6), BUDGET DECIMAL(9), CITY VARCHAR(15)) ;

INSERT INTO PROJ VALUES  ('P1','MXSS','Design',10000,'Deale')
,('P2','CALM','Code',30000,'Vienna') ,('P3','SDP','Test',30000,'Tampa') ,('P4','SDP','Design',20000,'Deale')
,('P5','IRM','Test',10000,'Vienna') ,('P6','PAYR','Design',50000,'Deale');
