/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static java.lang.String.format;

/**
 * Test constraints.
 *
 * @author Jeff Cunningham
 * Date: 6/10/13
 */
//@Category(SerialTest.class)
public class ConstraintConstantOperationIT {
    private static final String SCHEMA = ConstraintConstantOperationIT.class.getSimpleName().toUpperCase();
    private static final String ROLE1 = "CONS_TEST_ROLE1";
    private static final String ROLE2 = "CONS_TEST_ROLE2";
    private static final String USER1 = "CONS_TEST_USER";
    private static final String ROLE3 = "CONS_TEST_ROLE3";
    private static final String ROLE4 = "CONS_TEST_ROLE4";
    private static final String USER2 = "CONS_TEST_USER2";
    private static final String PASSWORD1 = "test";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceRoleWatcher spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    private static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD1);
    private static SpliceRoleWatcher spliceRoleWatcher3 = new SpliceRoleWatcher(ROLE3);
    private static SpliceRoleWatcher spliceRoleWatcher4 = new SpliceRoleWatcher(ROLE4);

    @ClassRule
    public static TestRule chain =
            RuleChain.outerRule(spliceClassWatcher)
                    .around(schemaWatcher)
                    .around(spliceUserWatcher1)
                    .around(spliceRoleWatcher1)
                    .around(spliceRoleWatcher2)
                    .around(spliceUserWatcher2)
                    .around(spliceRoleWatcher3)
                    .around(spliceRoleWatcher4);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    // primary key constraint
    // column-level check constraint
    private static final String EMP_PRIV_TABLE_NAME = "EMPPRIV";
    private static final String EMP_PRIV_TABLE_DEF =
        "(empId int not null CONSTRAINT EMP_ID_PK PRIMARY KEY, dob varchar(10) not null, ssn varchar(12) not null, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000))";
    protected static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE_NAME, SCHEMA, EMP_PRIV_TABLE_DEF);

    // foreign key constraint
    private static final String EMP_NAME_TABLE_NAME = "EMPNAME";
    private static final String EMP_NAME_TABLE_DEF = "(empId int not null CONSTRAINT EMP_ID_FK REFERENCES "+ empPrivTable+
        " ON UPDATE RESTRICT, fname varchar(8) not null, lname varchar(10) not null)";
    protected static SpliceTableWatcher empNameTable = new SpliceTableWatcher(EMP_NAME_TABLE_NAME, SCHEMA, EMP_NAME_TABLE_DEF);

    // table-level check constraint
    private static final String TASK_TABLE_NAME = "TASKS";
    private static final String TASK_TABLE_CONSTRAINT_NAME = "CHK_STARTEDAT_BEFORE_FINISHEDAT";
    private static final String TASK_TABLE_DEF =
        "(TaskId INT UNIQUE not null, empId int not null, StartedAt INT not null, FinishedAt INT not null, CONSTRAINT" +
            " " + TASK_TABLE_CONSTRAINT_NAME + " CHECK (StartedAt < FinishedAt))";
    protected static SpliceTableWatcher taskTable = new SpliceTableWatcher(TASK_TABLE_NAME, SCHEMA, TASK_TABLE_DEF);

    @AfterClass
    public static void tearDown() throws Exception {
        spliceClassWatcher.execute(String.format("drop role %s", ROLE1));
        spliceClassWatcher.execute(String.format("drop role %s", ROLE2));
        spliceClassWatcher.execute(String.format("call syscs_util.syscs_drop_user('%s')", USER1));
        spliceClassWatcher.execute(String.format("drop role %s", ROLE3));
        spliceClassWatcher.execute(String.format("drop role %s", ROLE4));
        spliceClassWatcher.execute(String.format("call syscs_util.syscs_drop_user('%s')", USER2));
    }

    @Before
    public void setupBefore() throws Exception {
        TestConnection conn=methodWatcher.getOrCreateConnection();
        new TableDAO(conn).drop(SCHEMA, EMP_NAME_TABLE_NAME, TASK_TABLE_NAME, EMP_PRIV_TABLE_NAME);

        new TableCreator(conn)
            .withCreate(String.format("create table %s %s", empPrivTable, EMP_PRIV_TABLE_DEF))
            .withInsert(String.format("insert into %s (EmpId, dob, ssn, salary) values (?,?,?,?)", empPrivTable))
                              .withRows(rows(row(100, "03/08", "777-22-1234", 10001))).create();

        new TableCreator(conn)
            .withCreate(String.format("create table %s %s", EMP_NAME_TABLE_NAME, EMP_NAME_TABLE_DEF))
            .withInsert(String.format("insert into %s (EmpId, fname, lname) values (?,?,?)", empNameTable))
            .withRows(rows(row(100, "'Fred'", "'Ziffle'"))).create();


        new TableCreator(conn)
            .withCreate(String.format("create table %s %s", TASK_TABLE_NAME, TASK_TABLE_DEF))
            .withInsert(String.format("insert into %s (TaskId, empId, StartedAt, FinishedAt) values (?,?,?,?)", taskTable))
            .withRows(rows(row(10, 100, 1400, 1430))).create();

    }

    /**
     * Test we get no exception when we insert following defined constraints.
     * @throws Exception
     */
    @Test
    public void testGoodInsertConstraint() throws Exception {
        String query = String.format("select * from %s", empPrivTable);
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s (EmpId, dob, ssn, salary) values (101, '04/08', '999-22-1234', 10001)",
                        empPrivTable));

        // insert good data
        statement.execute(
                String.format("insert into %s (EmpId, fname, lname) values (101, 'Jeff', 'Cunningham')",
                        empNameTable));

        ResultSet resultSet = connection.createStatement().executeQuery(query);
        Assert.assertTrue("Connection should see its own writes", resultSet.next());
    }

    /**
     * Test primary key constraint - we can't add row to a table where primary key already exist.
     * @throws Exception
     */
    @Test
    public void testBadInsertPrimaryKeyConstraint() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s (EmpId, dob, ssn, salary) values (102, '02/14', '444-33-4321', 10001)",
                              empPrivTable));

        // insert bad row - 102 empID in referenced table where PK constraint defined
        try {
            statement.execute(
                    String.format("insert into %s (EmpId, dob, ssn, salary) values (102, '03/14', '444-33-1212', 10001)",
                                  empPrivTable));
            Assert.fail("Expected exception inserting row with PK constraint violation.");
        } catch (SQLException e) {
            // expected
            Assert.assertEquals("The statement was aborted because it would have caused a duplicate key value in a " +
                                    "unique or primary key constraint or unique index identified by 'EMP_ID_PK' " +
                                    "defined on 'EMPPRIV'.",e.getLocalizedMessage());
        }
    }

    /**
     * Test foreign key constraint - we can't add row to a table where foreign key DNE reference.
     * @throws Exception
     */
    @Test
    public void testBadInsertForeignKeyConstraint() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s (EmpId, dob, ssn, salary) values (102, '02/14', '444-33-4321', 10001)",
                              empPrivTable));
        connection.commit();

        // insert bad row - no 103 empID in referenced table where FK constraint defined
        try {
            statement.execute(
                    String.format("insert into %s (EmpId, fname, lname) values (103, 'Bo', 'Diddly')", empNameTable));
            Assert.fail("Expected exception inserting row with FK constraint violation.");
        } catch (SQLException e) {
            // expected
           Assert.assertEquals("Operation on table 'EMPNAME' caused a violation of foreign key constraint " +
                                   "'EMP_ID_FK' for key (EMPID).  The statement has been rolled back.", e.getLocalizedMessage());
        }
    }

    /**
     * Test we can add a foreign key constraint to a table.
     * @throws Exception
     */
    @Test
    public void testAddForeignKeyConstraint() throws Exception {
        methodWatcher.getStatement().execute(String.format("alter table %s add foreign key (empId) references %s (empId)",
                                                           taskTable,  empPrivTable));
    }

    /**
     * Test we get an exception when violating a unique constraint - insert duplicate task ID.
     * @throws Exception
     */
    @Test
    public void testUniqueConstraint() throws Exception {
        TestConnection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                              taskTable, 1246, 101, 600, 700));

        // insert bad row - non-unique task ID
        try {
            statement.execute(
                    String.format("insert into %s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                                  taskTable, 1246, 102, 201, 300));
            Assert.fail("Expected exception inserting non-unique value on unique constrained col");
        } catch (SQLException e) {
            // expected
            // exception msg is printed with the generated name of the PK, so have to remove it from test
            String exMsg = e.getLocalizedMessage();
            String expectedMsgStart = "The statement was aborted because it would have caused a duplicate key value in a " +
                "unique or primary key constraint or unique index identified by 'SQL";
            String expectedMsgEnds = "' defined on 'TASKS'.";
            Assert.assertTrue(exMsg.startsWith(expectedMsgStart));
            Assert.assertTrue(exMsg.endsWith(expectedMsgEnds));
        }
    }

    /**
     * Test we insert good row on constrained table.
     * @throws Exception
     */
    @Test
    public void testGoodInsertTableConstraint() throws Exception {
        String query = String.format("select * from %s", taskTable);

        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                              taskTable, 1244, 101, 600, 700));

        ResultSet resultSet = statement.executeQuery(query);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
    }

    /**
     * Bug DB-966 - creating table with unique constraint gives java.util.UnknownFormatConversionException
     * @throws Exception
     */
    @Test
    public void testCreateTableUniqueConstraint() throws Exception {
        String TABLE_NAME = "t1";
        SpliceUnitTest.MyWatcher tableWatcher = new SpliceUnitTest.MyWatcher(TABLE_NAME, SCHEMA,
                        "(id int not null, name varchar(128) not null, constraint uq_t1 unique(id))");
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(SCHEMA, "testCreateTableUniqueConstraint"));
        Connection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        for (int i=1; i<20; i++) {
            statement.execute(String.format("insert into %s.%s values (%d,'%s')", SCHEMA, TABLE_NAME, i, "jeff"));
        }

//        String query = String.format("select * from %s order by id", tableWatcher);
//        ResultSet rs = methodWatcher.getOrCreateConnection().createStatement().executeQuery(query);
//        TestUtils.printResult(query, rs, System.out);
    }

    /**
     * Test we get an exception when violating a check constraint - start time after finish.
     * @throws Exception
     */
    @Test
//    @Ignore("DB-4641: failing when in Jenkins when run under the mem DB profile")
    public void testInsertRowCheckConstraintViolation() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert bad row - start time after finished time
        try {
            statement.execute(
                String.format("insert into %s (TaskId, empId, StartedAt, FinishedAt) values (%d,%d,%d,%d)",
                taskTable, 1245, 101, 700, 600));
            Assert.fail("Expected exception inserting check constraint violation.");
        } catch (SQLException e) {
        	String exMsg = e.getLocalizedMessage();
        	String expectedMsg =
                String.format("The check constraint '%s' was violated while performing an INSERT or UPDATE on table '%s.%s'.",
                              TASK_TABLE_CONSTRAINT_NAME, schemaWatcher.schemaName, TASK_TABLE_NAME);
            Assert.assertEquals(exMsg+" Expected:\n"+ expectedMsg, expectedMsg, exMsg);
        }
    }

    @Test
    public void testDropTableReferecedByFK() throws Exception {
        methodWatcher.execute("create table P (a int, b int, constraint pk1 primary key(a))");
        methodWatcher.execute("create table C (a int, CONSTRAINT fk1 FOREIGN KEY(a) REFERENCES P(a))");

        try {
            methodWatcher.execute("drop table P");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage().contains("Operation 'DROP CONSTRAINT' cannot be performed on object 'PK1' because CONSTRAINT 'FK1' is dependent on that object"));
        }

        methodWatcher.execute("drop table C");
        methodWatcher.execute("drop table P");
    }

    @Test
    public void testCreateTableWithFKConstraint() throws Exception {
        // this test makes sure that we don't add dependency of FK constraint on role descriptor and permission descriptor,
        // and that revoke role won't drop the FK automatically

        /*** TEST1 test dependency on role descriptor ***/
        // step 1: grant roles with permissions, and grant roles to the USER1
        TestConnection conn = spliceClassWatcher.createConnection();
        conn.execute(format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA, ROLE1));
        conn.execute(format("GRANT %s TO %s", ROLE1, USER1));
        conn.execute(format("GRANT %s TO %s", ROLE2, USER1));

        // step 2: create child table with PK-FK constraint using the privileges from the role
        TestConnection user1Conn = spliceClassWatcher.connectionBuilder().user(USER1).password(PASSWORD1).build();
        user1Conn.execute(format("create table %1$s.child (a2 int, b2 int, c2 int, constraint it_fkCons foreign key (a2) references %1$s.%2$s(empId))", SCHEMA, EMP_PRIV_TABLE_NAME));

        // step 3: check sys.sysdepends, there should only be one entry corresponding to the FK dependency on PK
        long c = conn.count("select * from sys.sysdepends as D, sys.sysconstraints as C where D.dependentId = C.constraintid and C.constraintname='IT_FKCONS'");
        Assert.assertTrue("rowcount incorrect", (c == 1));

        // step 4: revoke role from user, and check the existence of FK
        conn.execute(format("REVOKE %s from %s", ROLE1, USER1));

        c = conn.count("select * from sys.sysconstraints as C where C.constraintname='IT_FKCONS'");
        Assert.assertTrue("rowcount incorrect", (c == 1));

        // cleanup
        conn.execute(format("drop table %1$s.child", SCHEMA));
        conn.execute(format("revoke %s from %s", ROLE2, USER1));
        conn.execute(format("revoke all privileges on schema %s from %s", SCHEMA, ROLE1));
        user1Conn.close();

        /*** TEST2 test dependency on permission descriptor granted to user ***/
        // step 1: grant privileges to USER1
        conn.execute(format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA, USER1));

        // step 2: create child table with PK-FK constraint using the privileges from the role
        user1Conn = spliceClassWatcher.connectionBuilder().user(USER1).password(PASSWORD1).build();
        user1Conn.execute(format("create table %1$s.child (a2 int, b2 int, c2 int, constraint it_fkCons foreign key (a2) references %1$s.%2$s(empId))", SCHEMA, EMP_PRIV_TABLE_NAME));

        // step 3: check sys.sysdepends, there should only be one entry corresponding to the FK dependency on PK
        c = conn.count("select * from sys.sysdepends as D, sys.sysconstraints as C where D.dependentId = C.constraintid and C.constraintname='IT_FKCONS'");
        Assert.assertTrue("rowcount incorrect", (c == 1));

        // step 4: revoke privilege from user, and check the existence of FK
        conn.execute(format("REVOKE REFERENCES on SCHEMA %s from %s", SCHEMA, USER1));

        c = conn.count("select * from sys.sysconstraints as C where C.constraintname='IT_FKCONS'");
        Assert.assertTrue("rowcount incorrect", (c == 1));

        // cleanup
        conn.execute(format("drop table %s.child", SCHEMA));
        conn.execute(format("revoke all privileges on schema %s from %s", SCHEMA, USER1));

        conn.close();
        user1Conn.close();

    }

    @Test
    public void testCreateTableWithCheckConstraint() throws Exception {
        // this test makes sure that we only add the dependency of Check constraint on relevant role descriptor

        /*** TEST1 test dependency on role descriptor ***/
        TestConnection conn = spliceClassWatcher.createConnection();
        // step 1: create user defined function and load library
        // install jar file and set classpath
        String STORED_PROCS_JAR_FILE = System.getProperty("user.dir") + "/target/sql-it/sql-it.jar";
        String JAR_FILE_SQL_NAME = SCHEMA + "." + "SQLJ_IT_PROCS_JAR";
        conn.execute(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)", STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
        conn.execute(String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')", JAR_FILE_SQL_NAME));
        try {
            conn.execute("DROP FUNCTION SPLICE.WORD_LIMITER");
        } catch (SQLSyntaxErrorException e) {
            // the function may not exists, ignore this exception
        }
        conn.execute("CREATE FUNCTION SPLICE.WORD_LIMITER(\n" +
                "                    MY_SENTENCE VARCHAR(9999),\n" +
                "                    NUM_WORDS INT) RETURNS VARCHAR(9999)\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA\n" +
                "NO SQL\n" +
                "EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.wordLimiter'");


        // step 2: grant roles with permissions, and grant roles to the USER1
        conn.execute(format("grant execute on function splice.word_limiter to %s", ROLE3));
        conn.execute(format("GRANT %s TO %s", ROLE3, USER2));
        conn.execute(format("GRANT %s TO %s", ROLE4, USER2));

        // step 3: create child table with check constraint using the privileges from the role
        TestConnection user2Conn = spliceClassWatcher.connectionBuilder().user(USER2).password(PASSWORD1).build();
        user2Conn.execute(format("create table %s.tt(a1 int, b1 varchar(30), c1 int, constraint IT_check_cons1 CHECK(splice.word_limiter(b1, 5) > 'ABC'))", USER2));

        // step 4: check sys.sysdepends, check constraint should depend on only one role
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select R.roleid from sys.sysdepends as D, sys.sysconstraints as C, sys.sysroles as R " +
                "where D.dependentId = C.constraintid and C.constraintname='IT_CHECK_CONS1'" +
                "and D.providerId=R.UUID");
        String expected = "ROLEID      |\n" +
                "-----------------\n" +
                "CONS_TEST_ROLE3 |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        s.close();


        // step 5: revoke role from user, and check the existence of the check constraint
        // Note, this may not be the desired behavior, but it is the current behavior
        conn.execute(format("REVOKE %s from %s", ROLE3, USER2));

        long c = conn.count("select * from sys.sysconstraints as C where C.constraintname='IT_CHECK_CONS1'");
        Assert.assertTrue("rowcount incorrect", (c == 0));

        // cleanup
        user2Conn.close();
        conn.execute(format("drop table %s.tt", USER2));
        conn.execute(format("revoke %s from %s", ROLE4, USER2));
        conn.execute(format("revoke execute on function splice.word_limiter from %s restrict", ROLE3));
        conn.execute("DROP FUNCTION SPLICE.WORD_LIMITER");
        conn.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)");
        conn.execute(format("CALL SQLJ.REMOVE_JAR('%s', 0)", JAR_FILE_SQL_NAME));
        conn.close();

    }
}
