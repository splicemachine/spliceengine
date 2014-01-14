-- SQL Test Suite, V6.0, Schema Definition, schema5.std
-- 59-byte ID
-- TEd Version #
-- date_time print
-- *******************************************************************
-- ****** THIS FILE SHOULD BE RUN UNDER AUTHORIZATION ID FLATER ******
-- *******************************************************************

-- This is a standard schema definition.

--0 CREATE SCHEMA AUTHORIZATION FLATER
 CREATE SCHEMA FLATER;
  set schema FLATER;


   -- VIEW FR1 tests forward references in schema definitions.  This view
   -- was checked by test 0523 in SDL032; that test was removed prior to
   -- the release of V4.  I personally believe that two-pass SDL processing
   -- is the Right Thing and ought to be required, but I speak only for
   -- myself.
   -- CREATE VIEW FR1 AS SELECT * FROM DV1

   CREATE TABLE CONCATBUF (ZZ VARCHAR(240));
   CREATE TABLE USIG (C1 INT, C_1 INT);
   CREATE TABLE U_SIG (C1 INT, C_1 INT);

   CREATE VIEW DV1 AS
      SELECT DISTINCT HOURS FROM HU.WORKS;

   -- This small one-column table is used to generate an
   -- indicator overflow data exception for SQLSTATE testing.
   -- If the table cannot be created, the test is assumed passed.
   -- Save the error message and then use TEd to delete the CREATE TABLE
   -- as well as the GRANT ALL PRIVILEGES ON TINY TO SCHANZLE below.
   -- Use the following TEd change: del *schema5.[sop]* /TINY/
   -- Test number 0491 in program DML082 may also need to be deleted.
   CREATE TABLE TINY (C1 VARCHAR(33000));

   -- For generation of "with check option violation" SQLSTATE.
   CREATE TABLE BASE_WCOV (C1 INT);
   CREATE VIEW WCOV AS SELECT * FROM BASE_WCOV WHERE
--0     C1 > 0 WITH CHECK OPTION
     C1 > 0 ;

   CREATE TABLE BASE_VS1 (C1 INT, C2 INT);
   CREATE VIEW VS1 AS SELECT * FROM BASE_VS1 WHERE C1 = 0;
   CREATE VIEW VS2 AS
     SELECT A.C1 FROM BASE_VS1 A WHERE EXISTS
       (SELECT B.C2 FROM BASE_VS1 B WHERE B.C2 = A.C1);
   CREATE VIEW VS3 AS
     SELECT A.C2 FROM BASE_VS1 A WHERE A.C2 IN
       (SELECT B.C1 FROM BASE_VS1 B WHERE B.C1 < A.C2);
   CREATE VIEW VS4 AS
     SELECT A.C1 FROM BASE_VS1 A WHERE A.C1 < ALL
       (SELECT B.C2 FROM BASE_VS1 B);
   CREATE VIEW VS5 AS
     SELECT A.C1 FROM BASE_VS1 A WHERE A.C1 < SOME
       (SELECT B.C2 FROM BASE_VS1 B);
   CREATE VIEW VS6 AS
     SELECT A.C1 FROM BASE_VS1 A WHERE A.C1 < ANY
       (SELECT B.C2 FROM BASE_VS1 B);


--0   GRANT ALL PRIVILEGES ON TINY TO SCHANZLE
--0   GRANT ALL PRIVILEGES ON BASE_WCOV TO SCHANZLE
--0   GRANT ALL PRIVILEGES ON WCOV TO SCHANZLE
--0   GRANT ALL PRIVILEGES ON VS1 TO SCHANZLE


   -- Test granting of privileges that we don't have to start with.
   -- We have GRANT OPTION, but we should not be able to grant unrestricted
   -- update on STAFF3 since our own update is restricted to two columns.
   -- Do not change SCHEMA1 to grant unrestricted update.
   -- * expect error message *
--0   GRANT SELECT, UPDATE ON HU.STAFF3 TO SCHANZLE

   -- Same thing for views.
   -- * expect error message *
--0   GRANT SELECT, UPDATE ON HU.VSTAFF3 TO SCHANZLE

   -- See whether GRANT ALL PRIVILEGES gives you GRANT OPTION.
   -- It should not.  GRANT OPTION is not technically a privilege.
   -- * expect error message *
--0   GRANT SELECT ON CUGINI.BADG1 TO SCHANZLE

   -- See whether GRANT OPTION on a view gives you GRANT OPTION
   -- on the base table.
   -- * expect error message *
--0   GRANT SELECT ON CUGINI.BADG2 TO SCHANZLE

   -- Delimited identifiers.
   CREATE VIEW "SULLIVAN.SELECT" ("sullivan.select") AS
     SELECT C1 FROM BASE_VS1;
--0   GRANT ALL PRIVILEGES ON "SULLIVAN.SELECT" TO SCHANZLE
   
   -- Please be aware of the following errata; they are not being
   -- tested here.

   -- Check for erratum which allowed duplicate
   --   <unique constraint definition>s
   -- Reference ISO/IEC JTC1/SC21 N6789 section 11.7 SR7
   --   and Annex E #4
   --
   -- The following should be flagged or rejected:
   -- CREATE TABLE T0512 (C1 INT NOT NULL, C2 INT NOT NULL, C3 INT NOT NULL,
   --   UNIQUE (C1,C2), UNIQUE (C3), UNIQUE (C2,C1))
   CREATE TABLE T0512 (C1 INT NOT NULL, C2 INT NOT NULL, C3 INT NOT NULL,
   CONSTRAINT T0512_C1C2 UNIQUE (C1,C2), UNIQUE (C3),
   CONSTRAINT T0512_C2C1 UNIQUE (C2,C1));
   --0 PASS: if there was an error for a duplicate unique constraint

   -- Check for erratum which allowed recursive view definitions.
   -- Reference ISO/IEC JTC1/SC21 N6789 section 11.19 <view definition> SR4
   --   and Annex E #6
   --
   -- The following should be flagged or rejected:
   -- CREATE VIEW T0513 (C1, C2) AS
   --   SELECT T0513.C2, BASE_VS1.C1 FROM T0513, BASE_VS1
   CREATE VIEW T0513 (C1, C2) AS
   SELECT T0513.C2, BASE_VS1.C1 FROM T0513, BASE_VS1;
   --0 PASS: if an error is returned that the view is circular

-- ************* End of Schema *************

-- disconnect;
