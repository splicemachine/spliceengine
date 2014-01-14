AUTOCOMMIT OFF;

-- MODULE  YTS799  

-- SQL Test Suite, V6.0, Interactive SQL, yts799.sql
-- 59-byte ID
-- TEd Version #

-- AUTHORIZATION CTS1              
   set schema CTS1;

--O   SELECT USER FROM HU.ECCO;
  VALUES USER;
-- RERUN if USER value does not match preceding AUTHORIZATION comment
   ROLLBACK WORK;

-- date_time print

-- TEST:7531 <subquery> as <row val constr> in <null predicate>!

-- splicetest: ignore-order start

   SELECT TTA, TTB, TTC FROM CTS1.TT
     WHERE (SELECT TUD FROM TU WHERE TU.TUE = TT.TTA)
     IS NULL ORDER BY TTA DESC;
-- splicetest: ignore-order stop
-- PASS:7531 If 3 rows are selected in the following order?
--                  col1     col2     col3
--                  ====     ====     ====
-- PASS:7531 If     5        42       26  ?
-- PASS:7531 If     2        98       NULL?
-- PASS:7531 If     1        NULL     99  ?

-- splicetest: ignore-order start
   SELECT TTA, TTB, TTC FROM CTS1.TT
     WHERE (SELECT TUD FROM TU WHERE TU.TUE = TT.TTA)
     IS NOT NULL ORDER BY TTA;
-- splicetest: ignore-order stop
-- PASS:7531 If 2 rows are selected in the following order?
--                 col1     col1     col3
--                 ====     ====     ====
-- PASS:7531 If    3        97       96  ? 
-- PASS:7531 If    4        NULL     NULL?

--O   SELECT COUNT (*) FROM CTS1.TT
-- splicetest: ignore-order start
   SELECT * FROM CTS1.TT
     WHERE TTB IS NULL OR TTC IS NULL;
-- splicetest: ignore-order stop
-- PASS:7531 If COUNT = 3?

--O   SELECT COUNT (*) FROM CTS1.TT
-- splicetest: ignore-order start
   SELECT * FROM CTS1.TT
     WHERE TTB IS NOT NULL AND TTC IS NOT NULL;
-- splicetest: ignore-order stop
-- PASS:7531 If COUNT = 2?

--O   SELECT COUNT (*) FROM CTS1.TT
-- splicetest: ignore-order start
   SELECT * FROM CTS1.TT
     WHERE NOT (TTB IS NULL AND TTC IS NULL);
-- splicetest: ignore-order stop
-- PASS:7531 If COUNT = 4?

   ROLLBACK WORK;

-- END TEST >>> 7531 <<< END TEST
-- *********************************************
-- *************************************************////END-OF-MODULE
