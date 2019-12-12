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
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.*;
import java.util.*;

import static com.splicemachine.db.shared.common.reference.MessageId.SPLICE_GENERIC_EXCEPTION;
import static org.junit.Assert.*;

/**
 * Test WHEN clause in triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_When_Clause_IT extends SpliceUnitTest {


    private static final String SCHEMA = Trigger_When_Clause_IT.class.getSimpleName();
    public static final String CLASS_NAME = Trigger_When_Clause_IT.class.getSimpleName().toUpperCase();
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
    private static final String NOT_BOOLEAN = "42X19";
    private static final String HAS_PARAMETER = "42Y27";
    private static final String HAS_DEPENDENTS = "X0Y25";
    private static final String TRUNCATION = "22001";
    private static final String NOT_AUTHORIZED = "42504";
    private static final String NO_TABLE_PERMISSION = "42500";
    private static final String JAVA_EXCEPTION = "XJ001";
    private static final String NOT_SINGLE_COLUMN = "42X39";
    private static final String NON_SCALAR_QUERY = "21000";
    private static final String TRIGGER_RECURSION = "54038";
    private static final String PROC_USED_AS_FUNC = "42Y03";


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_When_Clause_IT(String connectionString ) {
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
            + "'org.splicetest.sqlj.SqlJTestProcs.intProcedure' no sql");
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

        c1 = classWatcher.createConnection("U1", "U1");
        c2 = classWatcher.createConnection("U2", "U2");
        c1.setAutoCommit(false);
        c1.setSchema(SCHEMA.toUpperCase());
        c2.setAutoCommit(false);
        c2.setSchema(SCHEMA.toUpperCase());
    }

    @After
    public void rollback() throws Exception{
        conn.rollback();
        //conn.reset();
        c1.rollback();
        //c1.reset();
        c2.rollback();
        //c2.reset();
    }

    @Test
    public void testBasicSyntax() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int)");
            s.executeUpdate("create table t2(y varchar(20))");

            // Create after triggers that should always be executed. Create row
            // trigger, statement trigger and implicit statement trigger.
            s.executeUpdate("create trigger tr01 after insert on t1 for each row "
            + "when (true) insert into t2 values 'Executed tr01'");
            s.executeUpdate("create trigger tr02 after insert on t1 for each statement "
            + "when (true) insert into t2 values 'Executed tr02'");
            s.executeUpdate("create trigger tr03 after insert on t1 "
            + "when (true) insert into t2 values 'Executed tr03'");

            // Create corresponding triggers that should never fire (their WHEN
            // clause is false).
            s.executeUpdate("create trigger tr04 after insert on t1 for each row "
            + "when (false) insert into t2 values 'Executed tr04'");
            s.executeUpdate("create trigger tr05 after insert on t1 for each statement "
            + "when (false) insert into t2 values 'Executed tr05'");
            s.executeUpdate("create trigger tr06 after insert on t1 "
            + "when (false) insert into t2 values 'Executed tr06'");

            // Create triggers with EXISTS subqueries in the WHEN clause. The
            // first returns TRUE and the second returns FALSE.
            s.executeUpdate("create trigger tr07 after insert on t1 "
            + "when (exists (select * from sysibm.sysdummy1)) "
            + "insert into t2 values 'Executed tr07'");
            s.executeUpdate("create trigger tr08 after insert on t1 "
            + "when (exists "
            + "(select * from sysibm.sysdummy1 where ibmreqd <> 'Y')) "
            + "insert into t2 values 'Executed tr08'");

            // WHEN clause returns NULL, trigger should not be fired.
            s.executeUpdate("create trigger tr09 after insert on t1 "
            + "when (cast(null as boolean))"
            + "insert into t2 values 'Executed tr09'");

            // WHEN clause contains reference to a transition variable.
            s.executeUpdate("create trigger tr10 after insert on t1 "
            + "referencing new as new for each row "
            + "when (new.x <> 2) insert into t2 values 'Executed tr10'");

            // WHEN clause contains reference to a transition table.
            // Needs DB-8883
//            s.executeUpdate("create trigger tr11 after insert on t1 "
//            + "referencing new table as new "
//            + "when (exists (select * from new where x > 5)) "
//            + "insert into t2 values 'Executed tr11'");
        }
        // Scalar subqueries are allowed in the WHEN clause, but they need an
        // extra set of parantheses.
        //
        // The first set of parantheses is required by the WHEN clause syntax
        // itself: WHEN ( <search condition> )
        //
        // The second set of parantheses is required by <search condition>.
        // Follow this path through the SQL standard's syntax rules:
        //    <search condition> -> <boolean value expression>
        //      -> <boolean term> -> <boolean factor> -> <boolean test>
        //      -> <boolean primary> -> <boolean predicand>
        //      -> <nonparenthesized value expression primary>
        //      -> <scalar subquery> -> <subquery> -> <left paren>
        String sqlText = "create trigger tr12 after insert on t1 "
                + "when (values true) insert into t2 values 'Executed tr12'";
        List<String> expectedErrors =
           Arrays.asList("Syntax error: Encountered \"values\" at line 1, column 46.");
        testUpdateFail(sqlText, expectedErrors, classWatcher);

        sqlText = "create trigger tr13 after insert on t1 "
                + "when (select true from sysibm.sysdummy1) "
                + "insert into t2 values 'Executed tr13'";
        expectedErrors =
           Arrays.asList("Syntax error: Encountered \"select\" at line 1, column 46.");
        testUpdateFail(sqlText, expectedErrors, classWatcher);
        try(Statement s = conn.createStatement()) {
            s.execute("create trigger tr12 after insert on t1 "
            + "when ((values true)) insert into t2 values 'Executed tr12'");
            s.execute("create trigger tr13 after insert on t1 "
            + "when ((select true from sysibm.sysdummy1)) "
            + "insert into t2 values 'Executed tr13'");

            // Now fire the triggers and verify the results.
            assertUpdateCount(s, 3, "insert into t1 values 1, 2, 3");

            String query = "select y, count(*) from t2 group by y order by y";
            String expected = "Y       | 2 |\n" +
            "-------------------\n" +
            "Executed tr01 | 3 |\n" +
            "Executed tr02 | 1 |\n" +
            "Executed tr03 | 1 |\n" +
            "Executed tr07 | 1 |\n" +
            "Executed tr10 | 2 |\n" +
            "Executed tr12 | 1 |\n" +
            "Executed tr13 | 1 |";
            testQuery(query, expected, s);

            // Empty t2 before firing the triggers again.
            s.execute("delete from t2");

            // Insert more rows with different values and see that a slightly
            // different set of triggers get fired.
            assertUpdateCount(s, 2, "insert into t1 values 2, 6");

            query = "select y, count(*) from t2 group by y order by y";
            expected = "Y       | 2 |\n" +
            "-------------------\n" +
            "Executed tr01 | 2 |\n" +
            "Executed tr02 | 1 |\n" +
            "Executed tr03 | 1 |\n" +
            "Executed tr07 | 1 |\n" +
            "Executed tr10 | 1 |\n" +
            "Executed tr12 | 1 |\n" +
            "Executed tr13 | 1 |";
            testQuery(query, expected, s);
        }
    }


    /**
     * A row trigger whose WHEN clause contains a subquery, used to cause a
     * NullPointerException in some situations.
     */
    @Test
    public void testSubqueryInWhenClauseNPE() throws SQLException {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int)");
            s.executeUpdate("create table t2(x int)");
            s.executeUpdate("create trigger tr1 after insert on t1 for each row "
            + "when ((values true)) insert into t2 values 1");

            // This statement used to result in a NullPointerException.
            s.executeUpdate("insert into t1 values 1,2,3");
        }
    }

    /**
     * Test generated columns referenced from WHEN clauses. In particular,
     * test that references to generated columns are disallowed in the NEW
     * transition variable of BEFORE triggers. See DERBY-3948.
     *
     * @see GeneratedColumnsTest#test_024_beforeTriggers()
     */
    @Ignore // DERBY-3948
    @Test
    public void testGeneratedColumns() throws Exception {
        createInt_Proc();
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int, y int, "
            + "z int generated always as (x+y))");
            s.executeUpdate("create table t2(x int)");

            // BEFORE INSERT trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr1 no cascade before insert on t1 "
            + "referencing new as new for each row when (new.x < new.y) "
            + "call INT_PROC(1)");

            // BEFORE INSERT trigger with generated column in WHEN clause, fail.
            testFail("42XAA",
            "create trigger btr2 no cascade before insert on t1 "
            + "referencing new as new for each row when (new.x < new.z) "
            + "select * from sysibm.sysdummy1", s);

            // BEFORE UPDATE trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr3 no cascade before update on t1 "
            + "referencing new as new old as old for each row "
            + "when (new.x < old.x) call int_proc(3)");

            // BEFORE UPDATE trigger with generated column in WHEN clause. OK,
            // since the generated column is in the OLD transition variable.
            s.executeUpdate("create trigger btr4 no cascade before update on t1 "
            + "referencing old as old for each row when (old.x < old.z) "
            + "call int_proc(4)");

            // BEFORE UPDATE trigger with generated column in NEW transition
            // variable, fail.
            testFail("42XAA",
            "create trigger btr5 no cascade before update on t1 "
            + "referencing new as new for each row when (new.x < new.z) "
            + "select * from sysibm.sysdummy1", s);

            // BEFORE DELETE trigger without generated column in WHEN clause, OK.
            s.executeUpdate("create trigger btr6 no cascade before delete on t1 "
            + "referencing old as old for each row when (old.x < 3) "
            + "call int_proc(6)");

            // BEFORE DELETE trigger with generated column in WHEN clause. OK,
            // since the generated column is in the OLD transition variable.
            s.executeUpdate("create trigger btr7 no cascade before delete on t1 "
            + "referencing old as old for each row when (old.x < old.z) "
            + "call int_proc(7)");

            // References to generated columns in AFTER triggers should always
            // be allowed.
            s.executeUpdate("create trigger atr1 after insert on t1 "
            + "referencing new as new for each row "
            + "when (new.x < new.z) insert into t2 values 1");
            s.executeUpdate("create trigger atr2 after update on t1 "
            + "referencing new as new old as old for each row "
            + "when (old.z < new.z) insert into t2 values 2");
            s.executeUpdate("create trigger atr3 after delete on t1 "
            + "referencing old as old for each row "
            + "when (old.x < old.z) insert into t2 values 3");

            // Finally, fire the triggers.
            s.execute("insert into t1(x, y) values (1, 2), (4, 3)");
            s.execute("update t1 set x = y");
            s.execute("delete from t1");

            // Verify that the before triggers were executed as expected.
            // No way to access this.
            //assertEquals(Arrays.asList(1, 3, 4, 4, 6, 7, 7), org.splicetest.sqlj.SqlJTestProcs.procedureCalls);

            // Verify that the after triggers were executed as expected.
            String query = "select * from t2 order by x";
            String expected = "Y      |\n" +
            "-------------------\n" +
            "1 |\n" +
            "2 |\n" +
            "3 |\n" +
            "3 |\n";
            testQuery(query, expected, s);
        }
    }

    /**
     * Test various illegal WHEN clauses.
     */
    @Test
    public void testIllegalWhenClauses() throws Exception {
        createInt_Proc();
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int)");
            s.executeUpdate("create table t2(x int)");

            // The WHEN clause expression must be BOOLEAN.
            testFail(NOT_BOOLEAN,
            "create trigger tr after insert on t1 "
            + "when (1) insert into t2 values 1", s);
            testFail(NOT_BOOLEAN,
            "create trigger tr after update on t1 "
            + "when ('abc') insert into t2 values 1", s);
            testFail(NOT_BOOLEAN,
            "create trigger tr after delete on t1 "
            + "when ((values 1)) insert into t2 values 1", s);
            testFail(NOT_BOOLEAN,
            "create trigger tr no cascade before insert on t1 "
            + "when ((select ibmreqd from sysibm.sysdummy1)) "
            + "call int_proc(1)", s);
            testFail(NOT_BOOLEAN,
            "create trigger tr no cascade before insert on t1 "
            + "when ((select ibmreqd from sysibm.sysdummy1)) "
            + "call int_proc(1)", s);
            testFail(NOT_BOOLEAN,
            "create trigger tr no cascade before update on t1 "
            + "referencing old as old for each row "
            + "when (old.x) call int_proc(1)", s);

            // Dynamic parameters (?) are not allowed in the WHEN clause.
            testFail(HAS_PARAMETER,
            "create trigger tr no cascade before delete on t1 "
            + "when (?) call int_proc(1)", s);
            testFail(HAS_PARAMETER,
            "create trigger tr after insert on t1 "
            + "when (cast(? as boolean)) call int_proc(1)", s);
            testFail(HAS_PARAMETER,
            "create trigger tr after delete on t1 "
            + "when ((select true from sysibm.sysdummy where ibmreqd = ?)) "
            + "call int_proc(1)", s);

            // Subqueries in the WHEN clause must have a single column
            testFail(NOT_SINGLE_COLUMN,
            "create trigger tr no cascade before insert on t1 "
            + "when ((values (true, false))) call int_proc(1)", s);
            testFail(NOT_SINGLE_COLUMN,
            "create trigger tr after update of x on t1 "
            + "when ((select tablename, schemaid from sys.systables)) "
            + "call int_proc(1)", s);
        }
    }

    /**
     * Test that dropping objects referenced from the WHEN clause will
     * detect that the trigger depends on the object.
     */
    @Test
    public void testDependencies() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            Savepoint sp = conn.setSavepoint();

            // Dropping columns referenced via the NEW transition variable in
            // a WHEN clause should fail.
            s.executeUpdate("create trigger tr after insert on t1 "
            + "referencing new as new for each row "
            + "when (new.x < new.y) values 1");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column y restrict");
            // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t1 drop column z restrict");
            //conn.rollback(sp);

            // Due to DB-8897 rollback doesn't work, so we
            // need to do manual cleanup.
            conn.commit();
            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            // Dropping columns referenced via the OLD transition variable in
            // a WHEN clause should fail.
            s.executeUpdate("create trigger tr no cascade before delete on t1 "
            + "referencing old as old for each row "
            + "when (old.x < old.y) values 1");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column y restrict");
            // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t1 drop column z restrict");
//            conn.rollback(sp);
            // Due to DB-8897 rollback doesn't work, so we
            // need to do manual cleanup.
            conn.commit();
            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            // Dropping columns referenced via either the OLD or the NEW
            // transition variable referenced in the WHEN clause should fail.
            s.executeUpdate("create trigger tr no cascade before update on t1 "
            + "referencing old as old new as new for each row "
            + "when (old.x < new.y) values 1");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column y restrict");
            // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t1 drop column z restrict");
//            conn.rollback(sp);
            // Due to DB-8897 rollback doesn't work, so we
            // need to do manual cleanup.
            conn.commit();
            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            // Dropping columns referenced either in the WHEN clause or in the
            // triggered SQL statement should fail.
            s.executeUpdate("create trigger tr no cascade before insert on t1 "
            + "referencing new as new for each row "
            + "when (new.x < 5) values new.y");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t1 drop column y restrict");
             // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t1 drop column z restrict");
//            conn.rollback(sp);
            // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t1 drop column z restrict");
//            conn.rollback(sp);
            // Due to DB-8897 rollback doesn't work, so we
            // need to do manual cleanup.
            conn.commit();
            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            // Dropping any column in a statement trigger with a NEW transition
            // table fails, even if the column is not referenced in the WHEN clause
            // or in the triggered SQL text.
            // Need DB-8883 for the following commented tests:
//            s.executeUpdate("create trigger tr after update of x on t1 "
//            + "referencing new table as new "
//            + "when (exists (select 1 from new where x < y)) values 1");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column x restrict");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column y restrict");
//            // Z is not referenced, but the transition table depends on all columns.
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column z restrict");
//            conn.rollback(sp);

            // Dropping any column in a statement trigger with an OLD transition
            // table fails, even if the column is not referenced in the WHEN clause
            // or in the triggered SQL text.
//            s.executeUpdate("create trigger tr after delete on t1 "
//            + "referencing old table as old "
//            + "when (exists (select 1 from old where x < y)) values 1");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column x restrict");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column y restrict");
//            // Z is not referenced, but the transition table depends on all columns.
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column z restrict");
//            conn.rollback(sp);

            // References to columns in other ways than via transition variables
            // or transition tables should also be detected.
//            s.executeUpdate("create trigger tr after delete on t1 "
//            + "referencing old table as old "
//            + "when (exists (select 1 from t1 where x < y)) values 1");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column x restrict");
//            assertStatementError(HAS_DEPENDENTS, s,
//            "alter table t1 drop column y restrict");
//            s.executeUpdate("alter table t1 drop column z restrict");
//            conn.rollback(sp);

            // References to columns in another table than the trigger table
            // should prevent them from being dropped.
            s.executeUpdate("create trigger tr after insert on t1 "
            + "when (exists (select * from t2 where x < y)) "
            + "values 1");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t2 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t2 drop column y restrict");
             // DB-8897 needed for this statement to succeed.
//            s.executeUpdate("alter table t2 drop column z restrict");

            // Dropping a table referenced in a WHEN clause should fail and leave
            // the trigger intact. Before DERBY-2041, DROP TABLE would succeed
            // and leave the trigger in an invalid state so that subsequent
            // INSERT statements would fail when trying to fire the trigger.
            //Need DERBY-2041 for the following:
            //assertStatementError(HAS_DEPENDENTS, s, "drop table t2");
            String query = format("select triggername from sys.systriggers t, sys.sysschemas s"
            + " where s.schemaid = t.schemaid and s.schemaname = '%s' ", SCHEMA.toUpperCase());
            String expected = "TRIGGERNAME |\n" +
            "--------------\n" +
            "     TR      |";
            testQuery(query, expected, s);

            s.executeUpdate("insert into t1 values (1, 2, 3)");
            // Uncomment after DB-8897 ships:
            //conn.rollback(sp);

            // Due to DB-8897 rollback doesn't work, so we
            // need to do manual cleanup.
            conn.commit();
            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.executeUpdate("create table t1(x int, y int, z int)");
            s.executeUpdate("create table t2(x int, y int, z int)");

            // Test references to columns in both the WHEN clause and the
            // triggered SQL statement.
            s.executeUpdate("create trigger tr after update on t1 "
            + "when (exists (select * from t2 where x < 5)) "
            + "select y from t2");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t2 drop column x restrict");
            assertStatementError(HAS_DEPENDENTS, s,
            "alter table t2 drop column y restrict");
            // DB-8897 needed for this statement to succeed.
            //s.executeUpdate("alter table t2 drop column z restrict");

            // DROP TABLE should fail because of the dependencies (didn't before
            // DERBY-2041).
            //Need DERBY-2041 for the following:
//            assertStatementError(HAS_DEPENDENTS, s, "drop table t2");
            query = format("select triggername from sys.systriggers t, sys.sysschemas s"
             + " where s.schemaid = t.schemaid and s.schemaname = '%s' ", SCHEMA.toUpperCase());
            expected = "TRIGGERNAME |\n" +
            "--------------\n" +
            "     TR      |";
            testQuery(query, expected, s);
        }
    }

    /**
     * Verify that DERBY-4874, which was fixed before support for the WHEN
     * clause was implemented, does not affect the WHEN clause.
     * The first stab at the WHEN clause implementation did suffer from it.
     */
    @Test
    public void testDerby4874() throws SQLException {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table t(x varchar(3))");
            s.executeUpdate("create trigger tr after update of x on t "
            + "referencing new as new for each row "
            + "when (new.x < 'abc') values 1");
            s.executeUpdate("insert into t values 'aaa'");

            // Updating X to something longer than 3 characters should fail,
            // since it's a VARCHAR(3).
            assertStatementError(TRUNCATION, s, "update t set x = 'aaaa'");

            // Change the type of X to VARCHAR(4) and try again. This time it
            // should succeed, but it used to fail because the trigger hadn't
            // been recompiled and still thought the max length was 3.
            s.executeUpdate("alter table t alter x set data type varchar(4)");
            assertUpdateCount(s, 1, "update t set x = 'aaaa'");

            // Updating it to a longer value should still fail.
            assertStatementError(TRUNCATION, s, "update t set x = 'aaaaa'");
        }
    }

    /**
     * Test for Derby-6783.
     */

    @Test
    public void testDerby6783() throws Exception {
        try(Statement s = conn.createStatement()) {

            s.execute("CREATE TABLE tabDerby6783(id INTEGER, result VARCHAR(10), status CHAR(1))");

            s.execute("CREATE TRIGGER trigger6783 AFTER UPDATE OF status ON tabDerby6783 "
            + "REFERENCING NEW AS newrow FOR EACH ROW WHEN (newrow.status='d') "
            + "UPDATE tabDerby6783 SET result='completed' WHERE id=newrow.id");
            s.execute("insert into tabDerby6783 values (1, null, 'a')");
            // Fire the trigger.
            s.execute("UPDATE tabDerby6783 SET status='d'");

            String query = "SELECT result FROM tabDerby6783";
            String expected = "RESULT   |\n" +
            "-----------\n" +
            "completed |";
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
            + " REFERENCING NEW AS newrow OLD AS oldrow"
            + " FOR EACH ROW WHEN ((oldrow.GRADE1 <> newrow.GRADE1 OR oldrow.GRADE2 <> newrow.GRADE2))"
            + " UPDATE tabDerby6783_1_1 SET TOTAL_MARKS = oldrow.MARKS1 + oldrow.MARKS2 where id=newrow.id");

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

            s.execute("CREATE TRIGGER trigger6783_1 AFTER UPDATE OF MARKS1 ON tabDerby6783_1_2 "
            + " REFERENCING NEW AS newrow OLD AS oldrow"
            + " FOR EACH ROW WHEN (oldrow.MARKS1 <> newrow.MARKS1)"
            + " UPDATE tabDerby6783_1_2 SET FINAL_GRADE = oldrow.GRADE1 where id=newrow.id");

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

            s.execute("CREATE TRIGGER trigger_2 AFTER UPDATE OF BALANCE ON tabDerby6783_2 "
            + " REFERENCING NEW AS newrow OLD AS oldrow"
            + " FOR EACH ROW WHEN (oldrow.RATE < 10.0)"
            + " UPDATE tabDerby6783_2 SET INTEREST = oldrow.balance + newrow.BALANCE * RATE");

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
            + " REFERENCING NEW AS newrow OLD AS oldrow"
            + " FOR EACH ROW WHEN (newrow.FIELD2 > 3000)"
            + " UPDATE tabDerby6783_3_2 SET FIELD3 = newrow.FIELD2 / 10");

            s.execute("UPDATE tabDerby6783_3_1 set FIELD1='hello'");

            String query = "SELECT FIELD3 FROM tabDerby6783_3_2";
            String expected = "FIELD3  |\n" +
            "----------\n" +
            "545456.7 |";
            testQuery(query, expected, s);

        }
    }

    /**
     * When SQL authorization is enabled, the trigger action (including the
     * WHEN clause) should execute with definer's rights. Verify that it is
     * so.
     */
    @Test
    public void testGrantRevoke() throws Exception {
        c1.setAutoCommit(true);
        conn.setAutoCommit(true);
        String spsValidQuery = format("select valid from sys.sysstatements s, sys.sysschemas sc where stmtname "
               + "like 'TRIGGERWHEN%%' and s.schemaid = sc.schemaid and schemaname = '%s'", SCHEMA.toUpperCase());

        Statement s = conn.createStatement();
        Statement s1 = c1.createStatement();
        s1.execute("create table t1(x varchar(20))");
        s1.execute("create table t2(x varchar(200))");
        s1.execute("create table t3(x int)");
        s1.execute("create function is_true(s varchar(128)) returns boolean "
                + "deterministic language java parameter style java "
                + "external name 'java.lang.Boolean.parseBoolean' no sql");

        //s.execute(format("grant execute on FUNCTION is_true to %s", USER1));

        // Trigger that fires on T1 if inserted value is 'true'.
        s.execute("create trigger tr1 after insert on t1 "
                + "referencing new as new for each row "
                + "when (is_true(new.x)) insert into t2(x) values new.x");

        // Trigger that fires on T1 on insert if T3 has more than 1 row.
        s1.execute("create trigger tr2 after insert on t1 "
                + "when (exists (select * from t3 offset 1 row)) "
                + "insert into t2(x) values '***'");

        // Allow U2 to insert into T1, but nothing else on U1's schema.
        s.execute("grant insert on table t1 to u2");

        c2.setAutoCommit(true);
        Statement s2 = c2.createStatement();

        // User U2 is not authorized to invoke the function IS_TRUE, but
        // is allowed to insert into T1.
        assertStatementError(NOT_AUTHORIZED, s2, format("values %s.is_true('abc')", SCHEMA));
        assertUpdateCount(s2, 4,
                "insert into t1(x) values 'abc', 'true', 'TrUe', 'false'");

        // Verify that the trigger fired. Since the trigger runs with
        // definer's rights, it should be allowed to invoke IS_TRUE in the
        // WHEN clause even though U2 isn't allowed to invoke it directly.
        String query = "select * from t2 order by x";
        String expected = "X  |\n" +
        "------\n" +
        "TrUe |\n" +
        "true |";
        testQuery(query, expected, s1);

        s1.execute("delete from t2");

        // Now test that TR1 will also fire, even though U2 isn't granted
        // SELECT privileges on the table read by the WHEN clause.
        s1.execute("insert into t3 values 1, 2");
        assertUpdateCount(s2, 2, "insert into t1(x) values 'x', 'y'");

        query = "select * from t2 order by x";
        expected = "X  |\n" +
        "-----\n" +
        "*** |";
        testQuery(query, expected, s1);

        s1.execute("delete from t2");

        // SPS is valid after triggers are fired.
        expected = "VALID |\n" +
        "--------\n" +
        " true  |\n" +
        " true  |";
        testQuery(spsValidQuery, expected, s);

        // Now invalidate the triggers and make sure they still work after
        // recompilation.
        s1.execute("alter table t1 alter column x set data type varchar(200)");


        // SPS is not valid after alter
        expected = "VALID |\n" +
        "--------\n" +
        " false |\n" +
        " false |";
        testQuery(spsValidQuery, expected, s);

        assertUpdateCount(s2, 2, "insert into t1(x) values 'true', 'false'");

        query = "select * from t2 order by x";
        expected = "X  |\n" +
        "------\n" +
        " *** |\n" +
        "true |";
        testQuery(query, expected, s1);

        // SPS is valid after trigger is fired and recompiled.
        expected = "VALID |\n" +
        "--------\n" +
        " true  |\n" +
        " true  |";
        testQuery(spsValidQuery, expected, s);

        s1.execute("delete from t2");

        // Revoke U2's insert privilege on T1.
        s.execute("revoke insert on table t1 from u2 ");

        // U2 should not be allowed to insert into T1 anymore.
        assertStatementError(NO_TABLE_PERMISSION, s2,
                             "insert into t1(x) values 'abc'");

        // U1 should still be allowed to do it (since U1 owns T1), and the
        // triggers should still be working.
        assertUpdateCount(s1, 2, "insert into t1(x) values 'true', 'false'");
        query = "select * from t2 order by x";
        expected = "X  |\n" +
        "------\n" +
        " *** |\n" +
        "true |";
        testQuery(query, expected, s1);

        s1.execute("delete from t2");

        // Now try to define a trigger in U2's schema that needs to invoke
        // IS_TRUE. Should fail because U2 isn't allowed to invoke it.
        s2.execute(format("create schema %s", USER2.toUpperCase()));
        s2.execute(format("set schema %s", USER2.toUpperCase()));
        s2.execute("create table t(x varchar(200))");
        assertStatementError(NOT_AUTHORIZED, s2,
                             format("create trigger tr after insert on t "
                             + "referencing new as new for each row "
                             + "when (%s.is_true(new.x)) values 1", SCHEMA));

        // Try again after granting execute permission to U2.
        s.execute("grant execute on function is_true to u2");
        s2.execute(format("create trigger tr after insert on t "
                + "referencing new as new for each row "
                + "when (%s.is_true(new.x)) values 1", SCHEMA));

        // Fire trigger.
        assertUpdateCount(s2, 3, "insert into t values 'ab', 'cd', 'ef'");

        // Revoking the execute permission will fail because the trigger
        // depends on it.
        assertStatementError(HAS_DEPENDENTS, s,
                "revoke execute on function is_true from u2 restrict");

        s2.execute("drop trigger tr");
        s2.execute("drop table t");
        s2.execute(format("drop schema %s restrict", USER2.toUpperCase()));
        s1.close();
        s2.close();

        s.execute("drop function is_true");

        s.close();

        c2.setAutoCommit(false);
        c1.setAutoCommit(false);

        conn.setAutoCommit(false);
        spliceSchemaWatcher.cleanSchemaObjects();
        createInt_Proc();
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
            + "referencing new as new for each row "
            + "when (f(new.x) < 100) insert into t2 values new.x");

            // Insert a value that causes Integer.parseInt() to throw a
            // NumberFormatException. The NFE will be wrapped in two SQLExceptions.
            assertStatementError(SPLICE_GENERIC_EXCEPTION, s,
            "insert into t1 values '1', '2', 'hello', '3', '121'");

            // The statement should be rolled back, so nothing should be in
            // either of the tables.
            this.assertTableRowCount("T1", 0, s);
            this.assertTableRowCount("T2", 0, s);

            // Now try again with values that don't cause exceptions.
            assertUpdateCount(s, 4, "insert into t1 values '1', '2', '3', '121'");

            // Verify that the trigger fired this time.
            String query = "select * from t2 order by x";
            String expected = "X |\n" +
            "----\n" +
            " 1 |\n" +
            " 2 |\n" +
            " 3 |";
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
            + "referencing new as new for each row "
            + "when ((select x > 0 from t3 where x = new.x)) "
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
     * Test that a WHEN clause can call the CURRENT_USER function.
     */
    @Test
    public void testCurrentUser() throws Exception {
        Statement s_main = conn.createStatement();
        Statement s = c1.createStatement();
        s_main.execute("create table t1(x int)");
        s_main.execute("create table t2(x varchar(10))");

        // Create one trigger that should only fire when current user is U2,
        // and one that should only fire when current user is different from
        // U2.
        s_main.execute("create trigger tr01 after insert on t1 "
                + "when (current_user = 'U2') "
                + "insert into t2 values 'TR01'");
        s_main.execute("create trigger tr02 after insert on t1 "
                + "when (current_user <> 'U2') "
                + "insert into t2 values 'TR02'");

        s_main.execute("grant insert on t1 to u2");
        conn.commit();

        // Used to get an assert failure or a NullPointerException here before
        // DERBY-6348. Expect it to succeed, and expect TR02 to have fired.
        s.execute("insert into t1 values 1");

        String query = "select * from t2";
        String expected = "X  |\n" +
        "------\n" +
        "TR02 |";
        testQuery(query, expected, s);

        c1.rollback();

        // Now try the same insert as user U2.
        c2.setAutoCommit(true);
        Statement s2 = c2.createStatement();
        s2.execute("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
        s2.execute("insert into T1"
            + " values 1");
        s2.close();

        // Since the insert was performed by user U2, expect TR01 to have fired.
        query = "select * from t2";
        expected = "X  |\n" +
        "------\n" +
        "TR01 |";
        testQuery(query, expected, s);

        // Cleanup.
        conn.commit();
        c2.setAutoCommit(false);
        s.close();
        s2.close();
    }

    /**
     * Test that a trigger with a WHEN clause can be recursive.
     */
    @Test
    public void testRecursiveTrigger() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t(x int)");
            s.execute("create trigger tr1 after insert on t "
            + "referencing new as new for each row "
            + "when (new.x > 0) insert into t values new.x - 1");

            // Now fire the trigger. This used to cause an assert failure or a
            // NullPointerException before DERBY-6348.
            s.execute("insert into t values 15, 1, 2");

            // The row trigger will fire three times, so that the above statement
            // will insert the values { 15, 14, 13, ... , 0 }, { 1, 0 } and
            // { 2, 1, 0 }.

            String query = "select * from t order by x";
            String expected = "X |\n" +
            "----\n" +
            " 0 |\n" +
            " 0 |\n" +
            " 0 |\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |\n" +
            "10 |\n" +
            "11 |\n" +
            "12 |\n" +
            "13 |\n" +
            "14 |\n" +
            "15 |\n" +
            " 2 |\n" +
            " 2 |\n" +
            " 3 |\n" +
            " 4 |\n" +
            " 5 |\n" +
            " 6 |\n" +
            " 7 |\n" +
            " 8 |\n" +
            " 9 |";
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
     * The WHEN clause text is stored in a LONG VARCHAR column in the
     * SYS.SYSTRIGGERS table. This test case verifies that the WHEN clause
     * is not limited to the usual LONG VARCHAR maximum length (32700
     * characters).
     */
    @Test
    public void testVeryLongWhenClause() throws Exception {
        Statement s = conn.createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");

        // Construct a WHEN clause that is more than 32700 characters.
        StringBuilder sb = new StringBuilder("(values /* a very");
        for (int i = 0; i < 10000; i++) {
            sb.append(", very");
        }
        sb.append(" long comment */ true)");

        String when = sb.toString();
        assertTrue(when.length() > 32700);

        s.execute("create trigger very_long_trigger after insert on t1 "
                + "when (" + when + ") insert into t2 values 1");

        // Verify that the WHEN clause was stored in SYS.SYSTRIGGERS.
        String query = "select whenclausetext from sys.systriggers "
                         + "where triggername = 'VERY_LONG_TRIGGER'";

        testQueryContains(query, "a very, very, very, very, very, very, very, very, very, very, very, very, very, very, very", s);

        // Verify that the trigger fires.
        s.execute("insert into t1 values 1");
        this.assertTableRowCount("T1", 1, s);
        this.assertTableRowCount("T2", 1, s);
        s.close();
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
            + "referencing new as new for each row "
            + "when (f(new.x)) insert into t2 values new.x");

            s.execute(format("insert into t1 values '%s.T3', '%s.T4', '%s.T3', '%s.T4', '%s.T3', '%s.T4'",
                SCHEMA, SCHEMA, SCHEMA, SCHEMA, SCHEMA, SCHEMA));

            String query = "select x, count(x) from t2 group by x";
            String expected = "X             | 2 |\n" +
            "-------------------------------\n" +
            "Trigger_When_Clause_IT.T4 | 3 |";
            testQuery(query, expected, s);

            spliceSchemaWatcher.cleanSchemaObjects();
        }
    }


    /**
     * <p>
     * SQL:2011, part 2, 11.49 &lt;trigger definition&gt;, syntax rule 11
     * says that the WHEN clause shall not contain routines that possibly
     * modifies SQL data. Derby does not currently allow functions to be
     * declared as MODIFIES SQL DATA. It does allow procedures to be declared
     * as MODIFIES SQL DATA, but the current grammar does not allow procedures
     * to be invoked from a WHEN clause. So there's currently no way to
     * invoke routines that possibly modifies SQL data from a WHEN clause.
     * </p>
     *
     * <p>
     * This test case verifies that it is not possible to declare a function
     * as MODIFIES SQL DATA, and that it is not possible to call a procedure
     * from a WHEN clause. If support for any of those features is added,
     * this test case will start failing as a reminder that code must be
     * added to prevent routines that possibly modifies SQL data from being
     * invoked from a WHEN clause.
     * </p>
     */
    @Test
    public void testRoutineModifiesSQLData() throws SQLException {
        try(Statement s = conn.createStatement()) {
            // Functions cannot be declared as MODIFIES SQL DATA currently.
            // Expect a syntax error.
            testFail(SYNTAX_ERROR,
                "create function f(x int) returns int language java "
                + "parameter style java external name 'java.lang.Math.abs' "
                + "modifies sql data", s);

            // Declare a procedure as MODIFIES SQL DATA.
            s.execute("create procedure p(i int) language java "
            + "parameter style java external name '"
            + getClass().getName() + ".intProcedure' no sql");

            // Try to call that procedure from a WHEN clause. Expect it to fail
            // because procedure invocations aren't allowed in a WHEN clause.
            s.execute("create table t(x int)");
            testFail(SYNTAX_ERROR,
            "create trigger tr after insert on t when (call p(1)) values 1", s);
            testFail(PROC_USED_AS_FUNC,
            "create trigger tr after insert on t when (p(1)) values 1", s);
        }
    }

    /**
     * Verify that aggregates (both built-in and user-defined) can be used
     * in a WHEN clause.
     */
    @Ignore("DB-8883")
    @Test
    public void testAggregates() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1(x int)");
            s.execute("create table t2(y varchar(10))");

            s.execute("create trigger tr1 after insert on t1 "
            + "referencing new table as new "
            + "when ((select max(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr1'");

            s.execute("create trigger tr2 after insert on t1 "
            + "referencing new table as new "
            + "when ((select count(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr2'");

            s.execute("create trigger tr3 after insert on t1 "
            + "referencing new table as new "
            + "when ((select mode_int(x) from new) between 0 and 3) "
            + "insert into t2 values 'tr3'");

            s.execute("insert into t1 values 2, 4, 4");

            String query = "select * from t2 order by y";
            String expected = "X  |\n" +
            "------\n" +
            "tr2 |";
            testQuery(query, expected, s);

            s.execute("delete from t2");

            s.execute("insert into t1 values 2, 2, 3, 1, 0");
            query = "select * from t2 order by y";
            expected = "X             | 2 |\n" +
            "-------------------------------\n" +
            "tr1 |" +
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
            "   REFERENCING OLD AS OLD NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (OLD.a = OLD.b)\n" +
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
            s.execute("INSERT INTO t1 values(2,'hello')");

            Savepoint sp = conn.setSavepoint();

            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS OLD NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (OLD.a = NEW.a)\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '87101' SET MESSAGE_TEXT = NEW.b CONCAT NEW.b;\n" +
            "END");


            List<String> expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"hello      hello      \"");

            testFail("UPDATE t1 SET a=2", expectedErrors, s);

            String query = "select * from t1";
            String expected = "A |  B   |\n" +
            "-----------\n" +
            " 1 |  1   |\n" +
            " 2 |hello |";
            testQuery(query, expected, s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   BEFORE UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS OLD NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (OLD.a = NEW.a)\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '87101' (OLD.a);\n" +
            "END");

            expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"2\"");

            testFail("UPDATE t1 SET a=2", expectedErrors, s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   BEFORE INSERT \n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (NEW.a > 0)\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '87101' ('You shall not pass!');\n" +
            "END");

            expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"You shall not pass!\"");

            testFail("insert into t1 values (1,1)", expectedErrors, s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS OLD NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (EXISTS (SELECT 1 from t2 where t2.a = OLD.a and t2.b = OLD.b))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '47584746' ;\n" +
            "END");

            expectedErrors =
            Arrays.asList("Application raised error or warning with diagnostic text: \"\"");
            testFail("UPDATE t1 SET a=2", expectedErrors, s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT \n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (EXISTS (SELECT 1 from t2 where t2.a = NEW.a and t2.b = NEW.b))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '4' ;\n" +
            "END");

            testFail("    4", "INSERT INTO t1 values (1,1)", s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT \n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (EXISTS (SELECT 1 from t2 where t2.a = NEW.a and t2.b = NEW.b))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE '492378654823' ;\n" +
            "END");

            testFail("49237", "INSERT INTO t1 values (1,1)", s);

            conn.rollback(sp);
            s.execute("CREATE TRIGGER mytrig\n" +
            "   AFTER INSERT \n" +
            "   ON t1\n" +
            "   REFERENCING NEW AS NEW\n" +
            "   FOR EACH ROW\n" +
            "   WHEN (EXISTS (SELECT 1 from t2 where t2.a = NEW.a and t2.b = NEW.b))\n" +
            "BEGIN ATOMIC\n" +
            "SIGNAL SQLSTATE 'ABC' ;\n" +
            "END");

            testFail("  ABC", "INSERT INTO t1 values (1,1)", s);
        }
    }

    @Test
    public void testSet() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1 (a int, b varchar(100))");
            s.execute("create table t2 (a int, b int)");
            s.execute("insert into t1 values(1,1)");
            s.execute("insert into t2 values(1,1)");
            s.execute("INSERT INTO t1 values(1,'hello')");

            Savepoint sp = conn.setSavepoint();

            List<String> expectedErrors =
            Arrays.asList("'SET' statements are not allowed in 'AFTER' triggers.");

            testFail("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS O NEW AS N\n" +
            "   FOR EACH ROW\n" +
            " WHEN (O.b = 'hello')" +
            "BEGIN ATOMIC\n" +
            "SET N.b = 'goodbye' ;" +
            "END", expectedErrors, s);

            expectedErrors =
            Arrays.asList("SET triggers may only reference NEW transition variables/tables.");

            testFail("CREATE TRIGGER mytrig\n" +
            "   AFTER UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS O NEW AS N\n" +
            "   FOR EACH ROW\n" +
            " WHEN (O.b = 'hello')" +
            "BEGIN ATOMIC\n" +
            "SET O.b = 'goodbye' ;" +
            "END", expectedErrors, s);

            s.execute("CREATE TRIGGER mytrig\n" +
            "   BEFORE UPDATE OF a,b\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS O NEW AS N\n" +
            "   FOR EACH ROW\n" +
            " WHEN (O.b LIKE 'hello%')" +
            "BEGIN ATOMIC\n" +
            "SET N.b = 'goodbye' ;" +
            "END");

            s.execute("UPDATE t1 SET a=2");

            String query = "select * from t1";
            String expected = "A |   B    |\n" +
            "-------------\n" +
            " 2 |   1    |\n" +
            " 2 |goodbye |";
            testQuery(query, expected, s);

            expectedErrors =
            Arrays.asList("DELETE triggers may only reference OLD transition variables/tables.");

            testFail("CREATE TRIGGER mytrig\n" +
            "   AFTER DELETE\n" +
            "   ON t1\n" +
            "   REFERENCING OLD AS O NEW AS N\n" +
            "   FOR EACH ROW\n" +
            " WHEN (O.b = 'hello')" +
            "BEGIN ATOMIC\n" +
            "SET N.b = 'goodbye' ;" +
            "END", expectedErrors, s);
        }
    }

    // Test Triggers with the SET statement that get recompiled.
    @Test
    public void test_DB_8974() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.execute("create table t1 (a timestamp, b int)");
            s.execute("insert into t1 values ({ts'2001-01-01 12:00:00'}, 1)");
            s.execute("CREATE TRIGGER TR1 NO CASCADE\n" +
                        "BEFORE UPDATE ON T1 REFERENCING NEW AS N\n" +
                        "FOR EACH ROW MODE DB2SQL\n" +
                        "WHEN (1=1)\n" +
                        "BEGIN ATOMIC\n" +
                        "   SET N.A = {ts'2005-01-01 23:00:00'};\n" +
                        "END");

            // Altering the table makes the trigger SPS in the
            // dictionary get marked as invalid to it will be
            // recompiled the next time it is invoked.
            s.execute("alter table t1 add column c int");

            s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()");
            s.execute("update t1 set a = {ts'2002-01-01 23:00:00'}");

            String query = "select a from t1";
            String expected = "A           |\n" +
            "-----------------------\n" +
            "2005-01-01 23:00:00.0 |";
            testQuery(query, expected, s);
        }
    }
}
