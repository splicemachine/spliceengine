/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Test constraints.
 *
 * @author Jeff Cunningham
 * Date: 6/10/13
 */
//@Category(SerialTest.class)
public class ConstraintConstantOperationIT {
    private static final String SCHEMA = ConstraintConstantOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

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
}
