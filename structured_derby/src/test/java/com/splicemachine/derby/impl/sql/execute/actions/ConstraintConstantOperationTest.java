package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test constraints.
 *
 * @author Jeff Cunningham
 *         Date: 6/10/13
 */
public class ConstraintConstantOperationTest {
    private static final String CLASS_NAME = ConstraintConstantOperationTest.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

    // primary key constraint
    // column-level check constraint
    private static final String EMP_PRIV_TABLE_NAME = "EmpPriv";
    private static final String EMP_PRIV_TABLE_DEF =
            "(empId int not null CONSTRAINT EMP_ID_PK PRIMARY KEY, dob varchar(10) not null, ssn varchar(12) not null, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000))";
    protected static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE_NAME,CLASS_NAME, EMP_PRIV_TABLE_DEF);

    // foreign key constraint
    private static final String EMP_NAME_TABLE_NAME = "EmpName";
    private static final String EMP_NAME_TABLE_DEF = "(empId int not null CONSTRAINT EMP_ID_FK REFERENCES "+
            CLASS_NAME+"."+EMP_PRIV_TABLE_NAME+" ON UPDATE RESTRICT, fname varchar(8) not null, lname varchar(10) not null)";
    protected static SpliceTableWatcher empNameTable = new SpliceTableWatcher(EMP_NAME_TABLE_NAME,CLASS_NAME, EMP_NAME_TABLE_DEF);

    // table-level check constraint
    private static final String TASK_TABLE_NAME = "Tasks";
    private static final String TASK_TABLE_DEF =
            "(TaskId INT UNIQUE not null, empId int not null, StartedAt INT not null, FinishedAt INT not null, CONSTRAINT CHK_StartedAt_Before_FinishedAt CHECK (StartedAt < FinishedAt))";
    protected static SpliceTableWatcher taskTable = new SpliceTableWatcher(TASK_TABLE_NAME,CLASS_NAME, TASK_TABLE_DEF);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(empPrivTable)
            .around(empNameTable)
            .around(taskTable);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Test we get no exception when we insert following defined constraints.
     * @throws Exception
     */
    @Test
    public void testGoodInsertConstraint() throws Exception {
        String query = String.format("select * from %s.%s", tableSchema.schemaName, EMP_NAME_TABLE_NAME);

        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s.%s (EmpId, dob, ssn, salary) values (101, '04/08', '999-22-1234', 10001)",
                        tableSchema.schemaName, EMP_PRIV_TABLE_NAME));
        connection.commit();

        // insert good data
        statement.execute(
                String.format("insert into %s.%s (EmpId, fname, lname) values (101, 'Jeff', 'Cunningham')",
                        tableSchema.schemaName, EMP_NAME_TABLE_NAME));
        connection.commit();

        ResultSet resultSet = connection.createStatement().executeQuery(query);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
        connection.commit();
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
                String.format("insert into %s.%s (EmpId, dob, ssn, salary) values (102, '02/14', '444-33-4321', 10001)",
                        tableSchema.schemaName, EMP_PRIV_TABLE_NAME));
        connection.commit();

        // insert bad row - no 103 empID in referenced table where FK constraint defined
        try {
            statement.execute(
                    String.format("insert into %s.%s (EmpId, dob, ssn, salary) values (102, '03/14', '444-33-1212', 10001)",
                            tableSchema.schemaName, EMP_PRIV_TABLE_NAME));
            Assert.fail("Expected exception inserting row with FK constraint violation.");
        } catch (SQLException e) {
            // expected
        }
    }

    /**
     * Test foreign key constraint - we can't add row to a table where foreign key DNE reference.
     * @throws Exception
     */
    @Test
    @Ignore("FK constraint not yet implemented")
    public void testBadInsertForeignKeyConstraint() throws Exception {

        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s.%s (EmpId, dob, ssn, salary) values (102, '02/14', '444-33-4321', 10001)",
                        tableSchema.schemaName, EMP_PRIV_TABLE_NAME));
        connection.commit();

        // insert bad row - no 103 empID in referenced table where FK constraint defined
        try {
            statement.execute(
                    String.format("insert into %s.%s (EmpId, fname, lname) values (103, 'Bo', 'Diddly')",
                            tableSchema.schemaName, EMP_NAME_TABLE_NAME));
            Assert.fail("Expected exception inserting row with FK constraint violation.");
        } catch (SQLException e) {
            // expected
        }
    }

    /**
     * Test we can add a foreign key constraint to a table.
     * @throws Exception
     */
    @Test
    @Ignore("FK constraint not yet implemented")
    public void testAddForeignKeyConstraint() throws Exception {
        methodWatcher.getStatement().execute(String.format("alter table %s.%s add foreign key (empId) references %s.%s (empId)",
                tableSchema.schemaName,
                TASK_TABLE_NAME,
                tableSchema.schemaName,
                EMP_PRIV_TABLE_NAME));
    }

    /**
     * Test we get an exception when violating a unique constraint - insert duplicate task ID.
     * @throws Exception
     */
    @Test
    public void testUniqueConstraint() throws Exception {
        Statement statement = methodWatcher.createConnection().createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s.%s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                        tableSchema.schemaName, TASK_TABLE_NAME, 1246, 101, 0600, 0700));

        // insert bad row - non-unique task ID
        try {
            statement.execute(
                    String.format("insert into %s.%s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                            tableSchema.schemaName, TASK_TABLE_NAME, 1246, 102, 0201, 0300));
            Assert.fail("Expected exception inserting non-unique value on unique constrained col");
        } catch (SQLException e) {
            // expected
        }
    }

    /**
     * Test we insert good row on constrained table.
     * @throws Exception
     */
    @Test
    public void testInsertGoodInsertTableConstraint() throws Exception {
        String query = String.format("select * from %s.%s", tableSchema.schemaName, TASK_TABLE_NAME);

        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert good data
        statement.execute(
                String.format("insert into %s.%s (TaskId, empId, StartedAt, FinishedAt) values (%d, %d,%d,%d)",
                        tableSchema.schemaName, TASK_TABLE_NAME, 1244, 101, 0600, 0700));

        ResultSet resultSet = statement.executeQuery(query);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());

        connection.commit();
    }

    /**
     * Test we get an exception when violating a check constraint - start time after finish.
     * @throws Exception
     */
    @Test
    @Ignore("Check Constraints not yet implemented.")
    public void testInsertBadRowIntoTableWithConstraint() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        // insert bad row - start time after finished time
        try {
            statement.execute(
                    String.format("insert into %s.%s (TaskId, empId, StartedAt, FinishedAt) values (%d,%d,%d,%d)",
                            tableSchema.schemaName, TASK_TABLE_NAME, 1245, 101, 0700, 0600));
            Assert.fail("Expected exception inserting check constraint violation.");
        } catch (SQLException e) {
            // expected
        }
        connection.commit();
    }
}
