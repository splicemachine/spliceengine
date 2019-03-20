-- Make sure predicate simplification is not turned off.
call syscs_util.syscs_set_global_database_property('derby.database.disablePredicateSimplification', null);

CREATE FUNCTION TO_DEGREES( RADIANS DOUBLE )
  RETURNS DOUBLE
  PARAMETER STYLE JAVA
  NO SQL
  LANGUAGE JAVA
  EXTERNAL NAME 'java.lang.Math.toDegrees';

create table A(a1 int, a2 int, a3 int);
create index pred_simpl_a1_a2 on a(a1,a2);
create table B(b1 int, b2 int, b3 int);
create index pred_simpl_b1 on b(b1);
create index pred_simpl_b2 on b(b2);
create table C(c1 int, c2 int, c3 int);
create index pred_simpl_c1 on c(c1);
create index pred_simpl_c2 on c(c2);
create table D(d1 int, d2 int, d3 int);
create index pred_simpl_d1 on d(d1);
create index pred_simpl_d2 on d(d2);

-- values that exist in all tables
INSERT INTO A VALUES (0, 0, 0),(1, 10, 100);
INSERT INTO B VALUES (0, 0, 0),(1, 10, 100);
INSERT INTO C VALUES (0, 0, 0),(1, 10, 100);
INSERT INTO D VALUES (0, 0, 0),(1, 10, 100);

-- combinations
INSERT INTO A VALUES (2, 20, 200), (3, 30, 300), (4, 40, 400), (5, 50, 500), (6, 60, 600), (7, 70, 700);
INSERT INTO B VALUES (2, 20, 200), (3, 30, 300),               (5, 50, 500), (6, 60, 600)                       ,(8, 80, 800), (9, 90, 900);
INSERT INTO C VALUES (2, 20, 200),                             (5, 50, 500)                           ,(8, 80, 800)             , (10, 100, 1000);
INSERT INTO D VALUES                                           (5, 50, 500), (6, 60, 600), (7, 70, 700);

-- duplicates
INSERT INTO A VALUES (11, 110, 1100);
INSERT INTO B VALUES (11, 110, 1100), (11, 110, 1100);
INSERT INTO C VALUES (11, 110, 1100), (11, 110, 1100), (11, 110, 1100);
INSERT INTO D VALUES (11, 110, 1100), (11, 110, 1100), (11, 110, 1100), (11, 110, 1100);

-- duplicates (in A)
INSERT INTO A VALUES (12, 120, 1200), (12, 120, 1200);
INSERT INTO D VALUES (12, 120, 1200), (12, 120, 1200);

-- nulls
INSERT INTO A VALUES (NULL, NULL, NULL);
INSERT INTO B VALUES (NULL, NULL, NULL), (NULL, NULL, NULL);
INSERT INTO C VALUES (NULL, NULL, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL);
INSERT INTO D VALUES (NULL, NULL, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL);

-- values that only exist in A
INSERT INTO A VALUES (13, 0, 0), (13, 1, 1);

-- DDL for DB-8081 test case
CREATE TABLE CONTACTS
(
NAME CHAR (20),
DESCRIPTION VARCHAR (1000),
KEYWORDS VARCHAR (1000)
);

INSERT INTO CONTACTS VALUES ('Harry','Harry works in the Redundancy Automation Division of the ' || 'Materials ' || 'Blasting Laboratory in the National Cattle Acceleration ' || 'Project of ' || 'lower Michigan.  His job is to document the trajectory of ' || 'cattle and ' || 'correlate the loft and acceleration versus the quality of ' || 'materials ' || 'used in the trebuchet.  He served ten years as the ' || 'vice-president in ' || 'charge of marketing in the now defunct milk trust of the ' || 'Pennsylvania ' || 'Coalition of All Things Bovine.  Prior to that he ' || 'established himself ' || 'as a world-class graffiti artist and source of all good ' || 'bits related ' || 'to channel dredging in poor weather.  He is author of over ' || 'ten thousand ' || 'paperback novels, including such titles as "How Many ' || 'Pumpkins will Fit ' || 'on the Head of a Pin," "A Whole Bunch of Useless Things ' || 'that you Don''t ' || 'Want to Know," and "How to Lift Heavy Things Over your ' || 'Head without ' || 'Hurting Yourself or Dropping Them."  He attends ANSI and ' || 'ISO standards ' || 'meetings in his copious free time and funds the development ' || 'of test ' || 'suites with his pocket change.','aardvark albatross nutmeg redundancy ' || 'automation materials blasting ' || 'cattle acceleration trebuchet catapult ' || 'loft coffee java sendmail SMTP ' || 'FTP HTTP censorship expletive senility ' || 'extortion distortion conformity ' || 'conformance nachos chicks goslings ' || 'ducklings honk quack melatonin tie ' || 'noose circulation column default ' || 'ionic doric chlorine guanine Guam ' || 'invasions rubicon helmet plastics ' || 'recycle HDPE nylon ceramics plumbing ' || 'parachute zeppelin carbon hydrogen ' || 'vinegar sludge asphalt adhesives ' || 'tensile magnetic Ellesmere Greenland ' || 'Knud Rasmussen precession ' || 'navigation positioning orbit altitude ' || 'resistance radiation levitation ' || 'yoga demiurge election violence ' || 'collapsed fusion cryogenics gravity ' || 'sincerity idiocy budget accounting ' || 'auditing titanium torque pressure ' || 'fragile hernia muffler cartilage ' || 'graphics deblurring headache eyestrain ' || 'interlace bandwidth resolution ' || 'determination steroids barrel oak wine ' || 'ferment yeast brewing bock siphon ' || 'clarity impurities SQL RBAC data ' || 'warehouse security integrity feedback');
