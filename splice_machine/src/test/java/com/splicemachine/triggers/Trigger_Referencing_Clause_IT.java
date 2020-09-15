/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.splicemachine.db.shared.common.reference.MessageId.SPLICE_GENERIC_EXCEPTION;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_TRIGGER_BAD_REF_MISMATCH;

/**
 * Test REFERENCING clause in triggers.
 */
@Ignore
@RunWith(Parameterized.class)
@Category({SerialTest.class, LongerThanTwoMinutes.class})
public class Trigger_Referencing_Clause_IT extends SpliceUnitTest {


    private static final String SCHEMA = Trigger_Referencing_Clause_IT.class.getSimpleName();
    public static final String CLASS_NAME = Trigger_Referencing_Clause_IT.class.getSimpleName().toUpperCase();
    protected static final String USER1 = "U1";
    protected static final String PASSWORD1 = "U1";
    protected static final String USER2 = "U2";
    protected static final String PASSWORD2 = "U2";

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);

    @ClassRule
    public static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD2);

    private TestConnection conn;
    private TestConnection c1;
    private TestConnection c2;

    private static final String SYNTAX_ERROR = "42X01";
    private static final String NON_SCALAR_QUERY = "21000";
    private static final String TRIGGER_RECURSION = "54038";


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_Referencing_Clause_IT(String connectionString ) {
        this.connectionString = connectionString;
    }

    protected void createInt_Proc() throws Exception {
        Connection c = conn;
        try(Statement s = c.createStatement()) {
            s.execute("create function f(x varchar(50)) returns boolean "
            + "language java parameter style java external name "
            + "'org.splicetest.sqlj.SqlJTestProcs.tableIsEmpty' reads sql data");

            s.executeUpdate("create procedure int_proc(i int) language java "
            + "parameter style java external name "
            + "'org.splicetest.sqlj.SqlJTestProcs.intProcedure' reads sql data");
            // If running tests in IntelliJ, use one of the commented-out versions of STORED_PROCS_JAR_FILE.
            String STORED_PROCS_JAR_FILE = System.getProperty("user.dir") + "/target/sql-it/sql-it.jar";
            //String STORED_PROCS_JAR_FILE = System.getProperty("user.dir") + "/../platform_it/target/sql-it/sql-it.jar";
            //String STORED_PROCS_JAR_FILE = System.getProperty("user.dir") + "/../mem_sql/target/sql-it/sql-it.jar";
            String JAR_FILE_SQL_NAME = CLASS_NAME + "." + "SQLJ_IT_PROCS_JAR";
            s.execute(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)", STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
            s.execute(String.format("CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')", JAR_FILE_SQL_NAME));
            c.commit();
        }
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        // grant schema privileges
        conn.execute(format("grant ALL PRIVILEGES on schema %s to %s", SCHEMA, USER1));
        conn.execute(format("grant access on schema %s to %s", SCHEMA, USER2));

        // grant execution privileges
        conn.execute(format("grant execute on procedure syscs_util.syscs_get_schema_info to %s", USER1));
        conn.execute(format("grant execute on procedure syscs_util.syscs_get_schema_info to %s", USER2));
        conn.execute(format("grant execute on procedure SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE to %s", USER1));
        conn.execute(format("grant execute on procedure SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE to %s", USER2));
        conn.execute(format("grant execute on procedure SYSCS_UTIL.INVALIDATE_DICTIONARY_CACHE to %s", USER1));
        conn.execute(format("grant execute on procedure SYSCS_UTIL.INVALIDATE_DICTIONARY_CACHE to %s", USER2));

        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA.toUpperCase());

        c1 = classWatcher.connectionBuilder().user("U1").password("U1").build();
        c2 = classWatcher.connectionBuilder().user("U2").password("U2").build();
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
    }

    @After
    public void rollback() throws Exception{
        conn.rollback();
        c1.rollback();
        c2.rollback();
    }

    @Test
    public void testBasicSyntax() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int)");
            s.executeUpdate("create table t2(y int)");

            testFail(SYNTAX_ERROR,
            "create trigger tr01 after insert on t1 " +
                    "referencing old table as old for each statement" +
                    "when (true) insert into t2 select x from old", s);

            s.executeUpdate("create trigger tr02 after insert on t1 " +
            " referencing new table as new for each statement "
            + "when (true) insert into t2 select x from new");

            testFail(SYNTAX_ERROR,
            "create trigger tr01 after insert on t1 " +
                    "referencing old_table as old for each statement" +
                    "when (true) insert into t2 select x from old", s);

            s.executeUpdate("create trigger tr04 after insert on t1 " +
            " referencing new_table as new for each statement "
            + "when (true) insert into t2 select x+x from new");

        }


        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create trigger tr05 after delete on t1 " +
            "referencing old table as old for each statement "
            + "when (true) insert into t2 select x from old");
            testFail(LANG_TRIGGER_BAD_REF_MISMATCH,
            "create trigger tr06 before delete on t1 " +
            " referencing new table as new for each statement "
            + "when (true) insert into t2 select x from new", s);

            // Now fire the triggers and verify the results.
            assertUpdateCount(s, 1, "insert into t1 values 1");

            String query = "select y from t2";
            String expected = "Y |\n" +
            "----\n" +
            " 1 |\n" +
            " 2 |";
            testQuery(query, expected, s);

            // Empty t2 before firing the triggers again.
            s.execute("delete from t2");

            // Insert more rows with different values and see that a slightly
            // different set of triggers get fired.
            assertUpdateCount(s, 1, "insert into t1 values 2");

            query = "select y from t2";
            expected = "Y |\n" +
            "----\n" +
            " 2 |\n" +
            " 4 |";
            testQuery(query, expected, s);
        }
    }


    /**
     * A statement trigger whose WHEN clause contains a subquery.
     */
    @Test
    public void testSubqueryInWhenClauseNPE() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int)");
            s.executeUpdate("create table t2(x int)");
            s.executeUpdate("create trigger tr1 after insert on t1 referencing new_table as new for each statement "
            + "when (exists (select x from new)) insert into t2 values 1");

            s.executeUpdate("insert into t1 values 1,2,3");

            String query = "select x from t2";
            String expected = "X |\n" +
            "----\n" +
            " 1 |";
            testQuery(query, expected, s);
        }
    }

    /**
     * Test generated columns referenced from WHEN clauses. In particular,
     * test that references to generated columns are disallowed in the NEW
     * transition variable of BEFORE triggers. See DERBY-3948.
     *
     */
    @Test
    public void testGeneratedColumns() throws Exception {
        createInt_Proc();
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int, y int, "
            + "z int generated always as (x+y))");
            s.executeUpdate("create table t2(x int)");

            // AFTER INSERT trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr1 after insert on t1 "
            + "referencing new table as new for each statement when (exists (select 1 from new where new.x < new.y)) "
            + "call INT_PROC(1)");

            // BEFORE INSERT trigger with generated column in WHEN clause, fail.
            testFail("42Y92",
            "create trigger btr2 no cascade before insert on t1 "
            + "referencing new table as new for each statement when (exists (select 1 from new where new.x < new.z)) "
            + "select * from sysibm.sysdummy1", s);

            // AFTER UPDATE trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr3 after update on t1 "
            + "referencing new table as new for each statement "
            + "when (exists (select 1 from new where new.x < new.x)) call int_proc(3)");

            // AFTER UPDATE trigger with generated column in WHEN clause. OK,
            // since the generated column is in the OLD transition variable.
            s.executeUpdate("create trigger btr4 after update on t1 "
            + "referencing old table as old for each statement "
            + "when (exists (select 1 from old where old.x < old.z)) "
            + "call int_proc(4)");

            // BEFORE UPDATE trigger with generated column in NEW transition
            // table, fail.
            testFail("42Y92",
            "create trigger btr5 no cascade before update on t1 "
            + "referencing new_table as new for each statement when (exists (select 1 from new where new.x < new.z))  "
            + "select * from sysibm.sysdummy1", s);

            // AFTER DELETE trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr6 after delete on t1 "
            + "referencing old table as old for each statement when (exists (select 1 from old where old.x < 3)) "
            + "call int_proc(6)");

            // AFTER DELETE trigger with generated column in WHEN clause. OK,
            // since the generated column is in the OLD transition table.
            s.executeUpdate("create trigger btr7 after delete on t1 "
            + "referencing old table as old for each statement when (exists (select 1 from old where old.x < old.z)) "
            + "call int_proc(7)");

            // References to generated columns in AFTER triggers should always
            // be allowed.
            s.executeUpdate("create trigger atr1 after insert on t1 "
            + "referencing new table as new for each statement "
            + "when (exists (select 1 from new where new.x < new.z)) insert into t2 values 1");

            s.executeUpdate("create trigger atr2 after update on t1 "
            + "referencing new table as new old table as old for each statement "
            + "when (exists (select 1 from new, old where new.x < old.x)) insert into t2 values 2");

            s.executeUpdate("create trigger atr3 after delete on t1 "
            + "referencing old table as old for each statement "
            + "when (exists (select 1 from old where old.x < old.z)) insert into t2 values 3");

            // Finally, fire the triggers.
            s.execute("insert into t1(x, y) values (1, 2), (4, 3)");
            //s.execute("update t1 set x = y");
            s.execute("delete from t1");

            // Verify that the after triggers were executed as expected.
            String query = "select * from t2 order by x";
            String expected = "X |\n" +
            "----\n" +
            " 1 |\n" +
            " 3 |";
            testQuery(query, expected, s);
        }
    }

    /**
     * Derby6783_1_1 test, this test has two trigger fields and
     * more than 3 column references in the update statement.
     */
    @Test
    public void testDerby6783_1_1() throws Exception
    {
        try(Statement s = conn.createStatement()) {

            s.execute("CREATE TABLE tabDerby6783_1_1(ID INTEGER, GRADE1 char(1), GRADE2 char(1),"
            + " MARKS1 integer, MARKS2 integer, TOTAL_MARKS integer)");

            s.execute("CREATE TRIGGER trigger6783_1 AFTER UPDATE OF GRADE1, GRADE2 ON tabDerby6783_1_1"
            + " REFERENCING NEW TABLE AS new OLD TABLE AS old"
            + " FOR EACH STATEMENT WHEN (exists (select 1 from old, new where old.GRADE1 <> new.GRADE1 OR old.GRADE2 <> new.GRADE2))"
            + " UPDATE tabDerby6783_1_1 SET TOTAL_MARKS = (select old.MARKS1 + new.MARKS2 from old, new where tabDerby6783_1_1.id=old.id and new.id = old.id)");

            s.execute("INSERT INTO tabDerby6783_1_1 VALUES (1, 'a', 'b', 30, 50, 0)");
            // Fire the trigger.
            s.execute("UPDATE tabDerby6783_1_1 SET GRADE1='b'");


            String query = "SELECT TOTAL_MARKS FROM tabDerby6783_1_1";
            String expected = "TOTAL_MARKS |\n" +
            "--------------\n" +
            "     80      |";
            testQuery(query, expected, s);

        }
    }

    /**
     * Derby6783_1_2 test, is a less complex version of Derby6783_1_1
     * It has only one column reference in trigger part and in update part.
     */
    @Test
    public void testDerby6783_1_2() throws Exception
    {
        try(Statement s = conn.createStatement()) {

            s.execute("CREATE TABLE tabDerby6783_1_2(ID INTEGER, GRADE1 char(1), GRADE2 char(1),"
            + " MARKS1 integer, MARKS2 integer, FINAL_GRADE char(1))");

            s.execute("CREATE TRIGGER trigger6783_1 AFTER UPDATE OF MARKS1, MARKS2 ON tabDerby6783_1_2"
            + " REFERENCING NEW TABLE AS new OLD TABLE AS old"
            + " FOR EACH STATEMENT WHEN (exists (select 1 from old, new where old.MARKS1 <> new.MARKS1))"
            + " UPDATE tabDerby6783_1_2 SET FINAL_GRADE = (select old.GRADE1 from old, new where tabDerby6783_1_2.id=old.id and new.id = old.id)");

            s.execute("INSERT INTO tabDerby6783_1_2 VALUES (1, 'a', 'b', 30, 50, 'c')");

            s.execute("UPDATE tabDerby6783_1_2 SET MARKS1=20");

            String query = "SELECT FINAL_GRADE FROM tabDerby6783_1_2";
            String expected = "FINAL_GRADE |\n" +
            "--------------\n" +
            "      a      |";
            testQuery(query, expected, s);

        }
    }

    /**
     * Derby6783_2 test, this test has a single trigger column reference
     * and two column reference in update statement. Also the when clause
     * has a different column reference than the trigger reference
    */
    @Test
    public void testDerby6783_2() throws Exception
    {
        try(Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tabDerby6783_2(ACC_NUMBER INT, BALANCE FLOAT, RATE REAL,"
            + " INTEREST REAL)");

            s.execute("CREATE TRIGGER trigger_2 AFTER UPDATE OF BALANCE ON tabDerby6783_2"
            + " REFERENCING NEW TABLE AS new OLD TABLE AS old"
            + " FOR EACH STATEMENT WHEN (exists (select 1 from old, new where old.RATE < 10.0))"
            + " UPDATE tabDerby6783_2 SET INTEREST = (select old.balance + new.balance * tabDerby6783_2.RATE from old, new where tabDerby6783_2.ACC_NUMBER=old.ACC_NUMBER and new.ACC_NUMBER = old.ACC_NUMBER)");

            s.execute("INSERT INTO tabDerby6783_2 VALUES (123, 12383.4534, 8.98, 2340)");

            s.execute("UPDATE tabDerby6783_2 SET BALANCE=22383.4543");

            s.execute("select INTEREST from tabDerby6783_2");

            String query = "SELECT INTEREST FROM tabDerby6783_2";
            String expected = "INTEREST  |\n" +
            "-----------\n" +
            "213386.88 |";
            testQuery(query, expected, s);

        }
    }

    /**
     * Derby6783_3 test, this test referes to different tables in
     * when clause and update clause.
    */
    @Test
    public void testDerby6783_3() throws Exception
    {
        try(Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tabDerby6783_3_1(FIELD1 VARCHAR(10),"
            + " FIELD2 DOUBLE)");

            s.execute("INSERT INTO tabDerby6783_3_1 VALUES ('helloworld', 5454567)");

            s.execute("CREATE TABLE tabDerby6783_3_2(FIELD3 NUMERIC (7,1))");

            s.execute("INSERT INTO tabDerby6783_3_2 VALUES (3.143)");

            s.execute("CREATE TRIGGER TRIGGER_3 AFTER UPDATE OF FIELD1 ON tabDerby6783_3_1"
            + " REFERENCING NEW TABLE AS new OLD Table AS old"
            + " FOR EACH STATEMENT WHEN (exists (select 1 from new where new.FIELD2 > 3000))"
            + " UPDATE tabDerby6783_3_2 SET FIELD3 = (select new.FIELD2 / 10 from new)");

            s.execute("UPDATE tabDerby6783_3_1 set FIELD1='hello'");

            String query = "SELECT FIELD3 FROM tabDerby6783_3_2";
            String expected = "FIELD3  |\n" +
            "----------\n" +
            "545456.7 |";
            testQuery(query, expected, s);

        }
    }

    /**
     * Test that the trigger fails gracefully if the WHEN clause throws
     * a RuntimeException.
     */
    @Test
    public void testRuntimeException() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create function f(x varchar(10)) returns int "
            + "deterministic language java parameter style java "
            + "external name 'java.lang.Integer.parseInt' no sql");
            s.execute("create table t1(x varchar(10))");
            s.execute("create table t2(x varchar(10))");
            s.execute("create trigger tr after insert on t1 "
            + "referencing new table as new for each statement "
            + "when (exists (select 1 from new where f(new.x) < 100)) insert into t2 select * from new");

            // Insert a value that causes Integer.parseInt() to throw a
            // NumberFormatException. The NFE will be wrapped in two SQLExceptions.
            assertStatementError(SPLICE_GENERIC_EXCEPTION, s,
            "insert into t1 values 'hello'");

            // The statement should be rolled back, so nothing should be in
            // either of the tables.
            this.assertTableRowCount("T1", 0, s);
            this.assertTableRowCount("T2", 0, s);

            // Now try again with values that don't cause exceptions.
            assertUpdateCount(s, 4, "insert into t1 values '1', '2', '3', '121'");

            // Verify that the trigger fired this time.
            String query = "select * from t2 order by x";
            String expected = "X  |\n" +
            "-----\n" +
            " 1  |\n" +
            "121 |\n" +
            " 2  |\n" +
            " 3  |";
            testQuery(query, expected, s);
        }
    }

    /**
     * Test that scalar subqueries are allowed, and that non-scalar subqueries
     * result in exceptions when the trigger fires.
     */
    @Test
    public void testScalarSubquery() throws SQLException {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1(x int)");
            s.execute("create table t2(x int)");
            s.execute("create table t3(x int)");

            s.execute("insert into t3 values 0,1,2,2");

            s.execute("create trigger tr1 after insert on t1 "
            + "referencing new table as new for each statement "
            + "when ((select x > 0 from t3 where x in (select new.x from new))) "
            + "insert into t2 values 1");

            // Subquery returns no rows, so the trigger should not fire.
            s.execute("insert into t1 values 42");
            this.assertTableRowCount("T2", 0, s);

            // Subquery returns a single value, which is false, so the trigger
            // should not fire.
            s.execute("insert into t1 values 0");
            this.assertTableRowCount("T2", 0, s);

            // Subquery returns a single value, which is true, so the trigger
            // should fire.
            s.execute("insert into t1 values 1");
            this.assertTableRowCount("T2", 1, s);

            // Subquery returns multiple values, so an error should be raised.
            assertStatementError(NON_SCALAR_QUERY, s, "insert into t1 values 2");
            this.assertTableRowCount("T2", 1, s);
        }
    }


    /**
     * Test that a trigger with a WHEN clause can be recursive.
     * Recursive triggers are currently broken on Spark...
     */
    @Ignore
    @Test
    public void testRecursiveTrigger() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t(x int)");
            s.execute("create trigger tr1 after insert on t "
            + "referencing new table as new for each statement "
            + "when (exists (select 1 from new where new.x > 0)) insert into t select new.x - 1 from new");

            // Now fire the trigger. This used to cause an assert failure or a
            // NullPointerException before DERBY-6348.
            s.execute("insert into t values 15, 1, 2");

            // The row trigger will fire three times, so that the above statement
            // will insert the values { 15, 14, 13, ... , 0 }, { 1, 0 } and
            // { 2, 1, 0 }.

            String query = "select * from t order by x";
            String expected = "X  |\n" +
            "-----\n" +
            "-1  |\n" +
            "-1  |\n" +
            "-10 |\n" +
            "-10 |\n" +
            "-11 |\n" +
            "-11 |\n" +
            "-12 |\n" +
            "-12 |\n" +
            "-13 |\n" +
            "-13 |\n" +
            "-14 |\n" +
            "-2  |\n" +
            "-2  |\n" +
            "-3  |\n" +
            "-3  |\n" +
            "-4  |\n" +
            "-4  |\n" +
            "-5  |\n" +
            "-5  |\n" +
            "-6  |\n" +
            "-6  |\n" +
            "-7  |\n" +
            "-7  |\n" +
            "-8  |\n" +
            "-8  |\n" +
            "-9  |\n" +
            "-9  |\n" +
            " 0  |\n" +
            " 0  |\n" +
            " 0  |\n" +
            " 1  |\n" +
            " 1  |\n" +
            " 1  |\n" +
            "10  |\n" +
            "11  |\n" +
            "12  |\n" +
            "13  |\n" +
            "14  |\n" +
            "15  |\n" +
            " 2  |\n" +
            " 2  |\n" +
            " 3  |\n" +
            " 4  |\n" +
            " 5  |\n" +
            " 6  |\n" +
            " 7  |\n" +
            " 8  |\n" +
            " 9  |";
            testQuery(query, expected, s);

            // Now fire the trigger with a value so that the maximum trigger
            // recursion depth (16) is exceeded, and verify that we get the
            // expected error.
            assertStatementError(TRIGGER_RECURSION, s, "insert into t values 16");

            // The contents of the table should not have changed, since the
            // above statement failed and was rolled back.
            testQuery(query, expected, s);
        }
    }

    /**
     * Test a WHEN clause that invokes a function declared with READ SQL DATA.
     */
    @Test
    public void testFunctionReadsSQLData() throws Exception {
        createInt_Proc();
        try(Statement s = conn.createStatement()) {

            s.execute("create table t1(x varchar(50))");
            s.execute("create table t2(x varchar(50))");
            s.execute("create table t3(x int)");
            s.execute("create table t4(x int)");
            s.execute("insert into t3 values 1");
            conn.commit();

            s.execute("create trigger tr after insert on t1 "
            + "referencing new table as new for each statement "
            + "when (exists (select 1 from new where f(new.x) = true)) insert into t2 select new.x from new");

            s.execute(format("insert into t1 values '%s.T3', '%s.T4', '%s.T3', '%s.T4', '%s.T3', '%s.T4'",
                SCHEMA, SCHEMA, SCHEMA, SCHEMA, SCHEMA, SCHEMA));

            String query = "select x, count(x) from t2 group by x";
            String expected = "X                | 2 |\n" +
            "--------------------------------------\n" +
            "Trigger_Referencing_Clause_IT.T3 | 3 |\n" +
            "Trigger_Referencing_Clause_IT.T4 | 3 |";
            testQuery(query, expected, s);

            spliceSchemaWatcher.cleanSchemaObjects();
        }
    }

    /**
     * Verify that aggregates (both built-in and user-defined) can be used
     * in a WHEN clause.
     */
    @Test
    public void testAggregates() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1(x int)");
            s.execute("create table t2(y varchar(10))");

            s.execute("create trigger tr1 after insert on t1 "
            + "referencing new table as new for each statement "
            + "when ((select max(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr1'");

            s.execute("create trigger tr2 after insert on t1 "
            + "referencing new table as new for each statement "
            + "when ((select count(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr2'");

            s.execute("create trigger tr3 after insert on t1 "
            + "referencing new table as new for each statement "
            + "when ((select min(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr3'");

            s.execute("insert into t1 values 2, 4, 4");

            String query = "select * from t2 order by y";
            String expected = "Y  |\n" +
            "-----\n" +
            "tr2 |\n" +
            "tr3 |";
            testQuery(query, expected, s);

            s.execute("delete from t2");

            s.execute("insert into t1 values 2, 2, 3, 1, 0");
            query = "select * from t2 order by y";
            expected = "Y  |\n" +
            "-----\n" +
            "tr1 |\n" +
            "tr3 |";
            testQuery(query, expected, s);

        }
    }

    @Test
    public void testBeginAtomic() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");
            s.execute("create table t2 (a int, b int, primary key(a))");
            s.execute("insert into t1 values(1,1)");
            s.execute("insert into t2 values(1,1)");

            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD TABLE AS OLD NEW TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "   WHEN (exists (select 1 from old where OLD.a = OLD.b))\n" +
            "BEGIN ATOMIC\n" +
            "insert into t2 values(1,1);\n" +
            "END");

            testFail("23505",
                     "UPDATE t1 SET a=4", s);

            String query = "select * from t1";
            String expected = "A | B |\n" +
            "--------\n" +
            " 1 | 1 |";
            testQuery(query, expected, s);
        }
    }

    @Test
    public void testSignal() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b char(11))");
            s.execute("create table t2 (a int, b int)");
            s.execute("insert into t1 values(1,1)");
            s.execute("insert into t2 values(1,1)");
            s.execute("insert into t2 values(2,2)");
            s.execute("insert into t2 values(2,3)");

            s.execute("INSERT INTO t1 values(2,'hello')");

            Savepoint sp = conn.setSavepoint();

            String sqltext = "CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD TABLE AS OLD NEW TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "   WHEN (exists (select 1 from old, new where OLD.a = NEW.a))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '87101' SET MESSAGE_TEXT = (select NEW.b from new) CONCAT (select NEW.b from new);\n" +
            "END";

            List<String> expectedErrors =
            Arrays.asList("'Subquery' statements are not allowed in 'SIGNAL' triggers.");

            testFail(sqltext, expectedErrors, s);

            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD TABLE AS OLD NEW TABLE AS NEW\n" +
            "   FOR EACH STATEMENT \n" +
            "   WHEN (exists (select 1 from old, new where OLD.a = NEW.a))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '87101' ('Hello World!');\n" +
            "END");

            expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"Hello World!\"");

            testFail("UPDATE t1 SET a=2", expectedErrors, s);

            s.execute("DROP TRIGGER mytrig");

            s.execute("CREATE TRIGGER mytrig " +
            "AFTER UPDATE OF A, B " +
            "ON t1 " +
            "REFERENCING NEW TABLE AS NEW " +
            "FOR EACH STATEMENT MODE DB2SQL " +
            "WHEN (1 < (SELECT COUNT(*) " +
            "                 FROM (SELECT DISTINCT a,b FROM t1 " +
            "                       WHERE a IN (SELECT a FROM NEW)) X)) " +
            "BEGIN ATOMIC " +
            "SIGNAL SQLSTATE '12345' ('UPDATE FAILED');\n" +
            "END");

            s.execute("delete from t1 where b = 'hello'");

            // The first update should succeed.
            s.execute("UPDATE t1 SET a=2 where a=1 and b=1");
            s.execute("insert into t1 values(3,3)");

            expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"UPDATE FAILED\"");

            // The second update should fail.
            testFail("UPDATE t1 SET a=3 where a=2 and b=1", expectedErrors, s);

            String query = "select * from t1";
            String expected = "A | B |\n" +
            "--------\n" +
            " 2 | 1 |\n" +
            " 3 | 3 |";
            testQuery(query, expected, s);
        }
    }

    @Test
    public void testNestedTriggers() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");
            s.execute("create table t2 (a int, b int)");
            s.execute("create table t3 (a int, b int)");
            s.execute("create table t4 (a int, b int)");
            s.execute("create table t5 (a int, b int)");

            s.execute("insert into t1 values(1,1)");
            s.execute("insert into t1 values(1,2)");

            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT\n" +
            "   ON t2\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t3 select new.a, new.b from new");

            s.execute("CREATE TRIGGER mytrig2\n" +
            "   AFTER INSERT\n" +
            "   ON t3\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t4 select new.a, new.b+1 from new\n" +
            " union all     select new.a, new.b+2 from new union all\n" +
            "               select new.a, new.b+3 from new");

            s.execute("CREATE TRIGGER mytrig3\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t5 select new.b - new.a, new.a from new");

            Savepoint sp = conn.setSavepoint();

            s.execute("insert into t2 select * from t1");

            String query = "select * from t2";
            String expected = "A | B |\n" +
            "--------\n" +
            " 1 | 1 |\n" +
            " 1 | 2 |";
            testQuery(query, expected, s);

            query = "select * from t3";
            expected = "A | B |\n" +
            "--------\n" +
            " 1 | 1 |\n" +
            " 1 | 2 |";
            testQuery(query, expected, s);

            query = "select * from t4";
            expected = "A | B |\n" +
            "--------\n" +
            " 1 | 2 |\n" +
            " 1 | 3 |\n" +
            " 1 | 3 |\n" +
            " 1 | 4 |\n" +
            " 1 | 4 |\n" +
            " 1 | 5 |";
            testQuery(query, expected, s);

            query = "select * from t5";
            expected = "A | B |\n" +
            "--------\n" +
            " 1 | 1 |\n" +
            " 2 | 1 |\n" +
            " 2 | 1 |\n" +
            " 3 | 1 |\n" +
            " 3 | 1 |\n" +
            " 4 | 1 |";
            testQuery(query, expected, s);

            conn.rollback(sp);
            expected = "";
            testQuery("select * from t2", expected, s);
            testQuery("select * from t3", expected, s);
            testQuery("select * from t4", expected, s);
            testQuery("select * from t5", expected, s);
        }
    }

    // Cause trigger rows to go over 1000000, causing a
    // rollback and switch to spark execution.
    @Test
    public void testNestedTriggersSwitchToSpark() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");
            s.execute("create table t2 (a int, b int)");
            s.execute("create table t3 (a int, b int)");
            s.execute("create table t4 (a int, b int)");
            s.execute("create table t5 (a int, b int)");

            s.execute("insert into t1 values(1,1)");
            s.execute("insert into t1 values(1,2)");
            s.execute("insert into t1 values(1,3)");
            s.execute("insert into t1 values(1,4)");
            s.execute("insert into t1 values(1,5)");
            s.execute("insert into t1 values(1,6)");
            s.execute("insert into t1 values(1,7)");
            s.execute("insert into t1 values(1,8)");
            s.execute("insert into t1 values(1,9)");
            s.execute("insert into t1 values(1,10)");

            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1");
            s.execute("insert into t1 select * from t1 --splice-properties useSpark=true\n");
            s.execute("insert into t1 select * from t1 --splice-properties useSpark=true\n");
            s.execute("insert into t1 select * from t1 --splice-properties useSpark=true\n");
            s.execute("insert into t1 select * from t1 --splice-properties useSpark=true\n");
            s.execute("insert into t1 select * from t1 --splice-properties useSpark=true\n");


            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT\n" +
            "   ON t2\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t3 select new.a, new.b from new");

            s.execute("CREATE TRIGGER mytrig2\n" +
            "   AFTER INSERT\n" +
            "   ON t3\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t4 select new.a, new.b+1 from new union all\n" +
            "               select new.a, new.b+2 from new union all\n" +
            "               select new.a, new.b+3 from new union all\n" +
            "               select new.a, new.b+4 from new union all\n" +
            "               select new.a, new.b+5 from new union all\n" +
            "               select new.a, new.b+6 from new");

            s.execute("CREATE TRIGGER mytrig3\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t5 select new.b - new.a, new.a from new");

            Savepoint sp = conn.setSavepoint();

            s.execute("insert into t2 select * from t1");

            String query = "select count(*) from t2";
            String expected = "1   |\n" +
            "--------\n" +
            "327680 |";
            testQuery(query, expected, s);

            query = "select count(*) from t3";
            testQuery(query, expected, s);

            query = "select count(*) from t4";
            expected = "1    |\n" +
            "---------\n" +
            "1966080 |";
            testQuery(query, expected, s);

            query = "select count(*) from t5";
            testQuery(query, expected, s);

            conn.rollback(sp);
            expected = "1 |\n" +
            "----\n" +
            " 0 |";
            testQuery("select count(*) from t2", expected, s);
            testQuery("select count(*) from t3", expected, s);
            testQuery("select count(*) from t4", expected, s);
            testQuery("select count(*) from t5", expected, s);
        }

    }

    @Test
    public void testConcurrentTriggers1() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");

            s.execute("CREATE TRIGGER tr1 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A * 3");

            s.execute("CREATE TRIGGER tr2 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A - 7");

            s.execute("CREATE TRIGGER tr3 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A * 2");

            s.execute("CREATE TRIGGER tr4 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A + 13");

            Savepoint sp = conn.setSavepoint();

            s.execute("INSERT INTO t1 VALUES (3,1)");
            s.execute("UPDATE T1 SET A=3");

            String query = "select * from t1";
            String expected = "A | B |\n" +
            "--------\n" +
            "17 | 1 |";
            testQuery(query, expected, s);

        }
    }

    @Test
    public void testConcurrentTriggers2() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");

            s.execute("CREATE TRIGGER tr1 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A * 3");

            s.execute("CREATE TRIGGER tr2 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A - 7");

            s.execute("CREATE TRIGGER tr3 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A * 2");

            s.execute("CREATE TRIGGER tr4 NO CASCADE\n" +
            "   BEFORE UPDATE OF A,B\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW MODE DB2SQL\n" +
            "SET NEW.A = NEW.A + 13");

            s.execute("CREATE TRIGGER tr5\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr6\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr7\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr8\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr9\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr10\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr11\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr12\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr13\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER tr14\n" +
            "   AFTER UPDATE\n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.a = 4)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            Savepoint sp = conn.setSavepoint();

            s.execute("INSERT INTO t1 VALUES (3,1)");
            s.execute("UPDATE T1 SET A=3");

            String query = "select * from t1";
            String expected = "A | B |\n" +
            "--------\n" +
            "17 | 1 |";
            testQuery(query, expected, s);

        }
    }

    @Test
    public void testConcurrentTriggers3() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b int)");
            s.execute("create table t2 (a int, b int)");
            s.execute("create table t3 (a int, b int)");
            s.execute("create table t4 (a int, b int)");
            s.execute("create table t5 (a int, b int)");
            s.execute("create table t6 (a int, b int)");

            s.execute("insert into t1 values (1,1)");
            s.execute("insert into t1 values (1,2)");
            s.execute("insert into t1 values (1,3)");
            s.execute("insert into t1 values (1,4)");
            s.execute("insert into t1 values (1,5)");
            s.execute("insert into t1 values (1,6)");
            s.execute("insert into t1 values (1,7)");
            s.execute("insert into t1 values (1,8)");
            s.execute("insert into t1 values (1,9)");
            s.execute("insert into t1 values (1,10)");

            s.execute("insert into t1 select a, b+10 from t1");
            s.execute("insert into t1 select a, b+20 from t1");
            s.execute("insert into t1 select a, b+40 from t1");
            s.execute("insert into t1 select a, b+80 from t1");
            s.execute("insert into t1 select a, b+160 from t1");
            s.execute("insert into t1 select a, b+320 from t1");
            s.execute("insert into t1 select a, b+640 from t1");



            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT\n" +
            "   ON t2\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t3 select new.a, new.b from new");

            s.execute("CREATE TRIGGER mytrig2\n" +
            "   AFTER INSERT\n" +
            "   ON t3\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t4 select new.a, new.b from new");

            s.execute("CREATE TRIGGER mytrig3\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW_TABLE AS NEW\n" +
            "   FOR EACH STATEMENT\n" +
            "insert into t5 select new.b - new.a, new.a from new");

            s.execute("CREATE TRIGGER mytrig4\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            s.execute("CREATE TRIGGER mytrig5\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER mytrig6\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            s.execute("CREATE TRIGGER mytrig7\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            s.execute("CREATE TRIGGER mytrig8\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER mytrig9\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            s.execute("CREATE TRIGGER mytrig10\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            s.execute("CREATE TRIGGER mytrig11\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'\n");

            s.execute("CREATE TRIGGER mytrig12\n" +
            "   AFTER INSERT\n" +
            "   ON t4\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "WHEN (NEW.b > 1000)\n" +
            "SIGNAL SQLSTATE '12345'");

            Savepoint sp = conn.setSavepoint();

            testFail("12345", "insert into t2 select * from t1", s);

        }
    }
}
