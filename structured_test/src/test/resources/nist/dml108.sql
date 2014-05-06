AUTOCOMMIT OFF;

-- MODULE  DML108  

-- SQL Test Suite, V6.0, Interactive SQL, dml108.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION FLATER
   set schema FLATER;

--0   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:0617 DATETIME with predicates, set fns (static)!

   CREATE TABLE TEMPS (
  ENTERED  TIMESTAMP,
  START    DATE,
--0  APPT     INTERVAL DAY,
--0  APPT     DATE,
  HOUR_IN  TIME,
  HOUR_OUT TIME
--0  , LUNCH    INTERVAL HOUR TO MINUTE);
--0  , LUNCH  TIME );
-- PASS:0617 If table is created?
);
   COMMIT WORK;

   CREATE VIEW SUBQ1 AS
  SELECT MIN (HOUR_IN) AS TOO_EARLY,
  MAX (ALL START) AS LATEST
-- AR Uncomment the following
--  , AVG (LUNCH) AS AVGLUNCH,
--0  , AVG (TIME(LUNCH)) AS AVGLUNCH,
--  AVG (DISTINCT LUNCH) AS D_AVGLUNCH,
--  AVG (DISTINCT TIME(LUNCH)) AS D_AVGLUNCH,
--0  SUM (APPT) AS SUMAPPT
--  SUM (DAY(APPT)) AS SUMAPPT
  FROM TEMPS;
-- PASS:0617 If view is created?

   COMMIT WORK;

   INSERT INTO TEMPS VALUES (
  TIMESTAMP( '1993-11-10 12:25:14'),
  DATE( '1993-11-12'),
--  cast('0000-00-04' as date),
--0  INTERVAL '4' DAY,
  TIME( '08:30:00'),
  TIME( '16:30:00')
--0  , INTERVAL '1:00' HOUR TO MINUTE);
--  , cast('1:00:00' as time)
  );
-- PASS:0617 If 1 row is inserted?

   INSERT INTO TEMPS VALUES (
  TIMESTAMP( '1993-11-10 13:15:14'),
  DATE( '1993-11-15'),
--0  INTERVAL '5' DAY,
--  cast ('5' as timestamp), 
  TIME( '08:30:00'),
  TIME( '17:30:00')
--0  ,INTERVAL '0:30' HOUR TO MINUTE)
-- , cast('0:30' as time)
  );
-- PASS:0617 If 1 row is inserted?

   INSERT INTO TEMPS VALUES (
  TIMESTAMP( '1993-11-17 09:56:48'),
  DATE( '1994-11-18'),
--0  INTERVAL '3' DAY,
--  cast('3'as timestamp),
  TIME( '09:00:00'),
  TIME( '17:00:00')
--0  ,INTERVAL '1:00' HOUR TO MINUTE)
-- ,cast('1:00' as time)
  );
-- PASS:0617 If 1 row is inserted?

--0   SELECT COUNT(*)
--0  FROM TEMPS WHERE
--0  LUNCH < INTERVAL '1:00' HOUR TO MINUTE;
-- PASS:0617 If count = 1?

--0   SELECT COUNT(*)
--0  FROM TEMPS WHERE
--0  LUNCH <= INTERVAL '1:00' HOUR TO MINUTE;
-- PASS:0617 If count = 3?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  START <> DATE( '1993-11-15') AND
  START <> DATE( '1993-11-12');
-- PASS:0617 If count = 1?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  START = DATE( '1993-11-15') OR
  START = DATE( '1993-11-12');
-- PASS:0617 If count = 2?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  HOUR_OUT > TIME( '17:00:00');
-- PASS:0617 If count = 1?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  HOUR_OUT >= TIME( '17:00:00');
-- PASS:0617 If count = 2?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  ENTERED BETWEEN TIMESTAMP( '1993-11-10 00:00:00' )AND
                  TIMESTAMP( '1993-11-10 23:59:59');
-- PASS:0617 If count = 2?

--0   SELECT COUNT(*)
--0  FROM TEMPS WHERE
--0  HOUR_OUT IN
--0  (SELECT HOUR_IN + INTERVAL '8' HOUR FROM TEMPS);
-- PASS:0617 If count = 2?

--0   SELECT COUNT(*)
--0  FROM TEMPS WHERE
--0  (START, APPT) OVERLAPS
--0  (DATE( '1993-11-14'), INTERVAL '2' DAY);
-- PASS:0617 If count = 2?

--0   SELECT COUNT(*)
--0  FROM TEMPS WHERE
--0  HOUR_OUT = ANY
--0  (SELECT HOUR_IN + INTERVAL '8' HOUR FROM TEMPS);
-- PASS:0617 If count = 2?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  YEAR( ENTERED) <> SOME
  (SELECT YEAR( START)
  FROM TEMPS);
-- PASS:0617 If count = 3?

   SELECT COUNT(*)
  FROM TEMPS WHERE
  YEAR( START) <> ALL
  (SELECT YEAR( ENTERED)
   FROM TEMPS);
-- PASS:0617 If count = 1?

   SELECT HOUR( TOO_EARLY)
  * 100 + MINUTE( TOO_EARLY)
  FROM SUBQ1;
-- PASS:0617 If 1 row selected and value is 830?

   SELECT YEAR( LATEST),
  MONTH( LATEST) * 100 +
  DAY( LATEST)
  FROM SUBQ1;
-- PASS:0617 If 1 row selected and values are 1994, 1118?

--0   SELECT HOUR( AVGLUNCH)
--0  * 100 + MINUTE( AVGLUNCH)
--0  FROM SUBQ1;
-- PASS:0617 If 1 row selected and value is 49 or 50?
-- NOTE:0617 50 is better but 49 is acceptable.

--0   SELECT HOUR( D_AVGLUNCH)
--0  * 100 + MINUTE( D_AVGLUNCH)
--0  FROM SUBQ1;
-- PASS:0617 If 1 row selected and value is 45?

--0   SELECT DAY( SUMAPPT)
--0  FROM SUBQ1;
-- PASS:0617 If 1 row selected and value is 12?

--0   SELECT COUNT (DISTINCT LUNCH) FROM TEMPS;
-- PASS:0617 If count = 2?

   ROLLBACK WORK;

--0   DROP TABLE TEMPS CASCADE;
   DROP TABLE SUBQ1 ;
   DROP TABLE TEMPS ;
-- PASS:0617 If table and view are dropped?

   COMMIT WORK;

-- END TEST >>> 0617 <<< END TEST

-- *************************************************////END-OF-MODULE
