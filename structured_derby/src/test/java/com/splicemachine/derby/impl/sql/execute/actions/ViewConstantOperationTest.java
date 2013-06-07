package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 6/6/13
 */
public class ViewConstantOperationTest extends SpliceUnitTest {
    private static List<String> empNameVals = Arrays.asList(
            "(001,'Jeff','Cunningham')",
            "(002,'Bill','Gates')",
            "(003,'John','Jones')",
            "(004,'Warren','Buffet')",
            "(005,'Tom','Jones')");

    private static List<String> empPrivVals = Arrays.asList(
            "(001,'04/08/1900','555-123-4567')",
            "(002,'02/20/1999','555-123-4577')",
            "(003,'11/31/2001','555-123-4587')",
            "(004,'06/05/1985','555-123-4597')",
            "(005,'09/19/1968','555-123-4507')");

    public static final String CLASS_NAME = ViewConstantOperationTest.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String EMP_NAME_TABLE = "emp_name";
    public static final String EMP_PRIV_TABLE = "emp_priv";

    public static final String VIEW_NAME_1 = "emp_full1";
    public static final String VIEW_NAME_2 = "emp_full2";

    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher viewSchema = new SpliceSchemaWatcher(CLASS_NAME+"_View");

    private static String eNameDef = "(id int not null, fname varchar(8) not null, lname varchar(10) not null)";
    private static String ePrivDef = "(id int not null, dob varchar(10) not null, ssn varchar(12) not null)";
    protected static SpliceTableWatcher empNameTable = new SpliceTableWatcher(EMP_NAME_TABLE,CLASS_NAME, eNameDef);
    protected static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE,CLASS_NAME, ePrivDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(viewSchema)
            .around(empNameTable)
            .around(empPrivTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        //  load emp_name table
                        for (String rowVal : empNameVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empNameTable.toString() + " values " + rowVal);
                        }

                        //  load emp_priv table
                        for (String rowVal : empPrivVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empPrivTable.toString() + " values " + rowVal);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Basic view create / drop.
     * @throws Exception
     */
    @Test
    public void testViewCreateDrop() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        // create
        connection1.createStatement().execute(String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                tableSchema.schemaName,
                VIEW_NAME_1,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE)));
        ResultSet resultSet = connection1.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
        Assert.assertEquals(5, resultSetSize(resultSet));

        // drop
        connection1.createStatement().execute(String.format("drop view %s.%s", tableSchema.schemaName, VIEW_NAME_1));
        connection1.commit();

        // attempt read
        try {
            resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", VIEW_NAME_1));
        } catch (SQLException e) {
            // expected
        }
        Assert.assertEquals(0, resultSetSize(resultSet));
    }

    /**
     * Within txn, create view, commit. Then read from view - good.
     * Set autocommit back to true and exec drop.
     *
     * @throws Exception
     */
    @Test
    public void testViewDropWithAutoCommit() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        // create
        connection1.createStatement().execute(String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                tableSchema.schemaName,
                VIEW_NAME_1,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE)));
        connection1.commit();

        // query
        ResultSet rs = null;
        try {
            rs = connection1.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
        } catch (Exception e) {
            e.printStackTrace(System.err);
            Assert.fail(e.getLocalizedMessage());
        }
        Assert.assertTrue(rs.next());

        // drop - autocommit
        connection1.setAutoCommit(true);
        try {
            connection1.createStatement().execute(String.format("drop view %s.%s", tableSchema.schemaName, VIEW_NAME_1));
            System.out.println("Drop view successful?");
        } catch (SQLException e) {
            e.printStackTrace(System.err);
            Assert.fail(e.getLocalizedMessage());
        }

        // query, but view should be gone
        try {
            connection1.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
            Assert.fail("Expected an exception but didn't get one.");
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * Within txn, create view, attempt to query (w/o prefixing view name, say, error) - get exception, catch, move on.
     * Drop view and explicitly commit.  View is gone.
     *
     * @throws Exception
     */
    @Test
    public void testViewDropExplicitCommit() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        // create
        connection1.createStatement().execute(String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                tableSchema.schemaName,
                VIEW_NAME_1,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE)));
        connection1.commit();

        // good query here
        try {
            connection1.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
        } catch (Exception e) {
            e.printStackTrace(System.err);
            Assert.fail(e.getLocalizedMessage());
        }

        // bad query here - did not prefix view name w/ schema
        try {
            connection1.createStatement().executeQuery(String.format("select * from %s", VIEW_NAME_1));
            Assert.fail("Expected an exception but didn't get one.");
        } catch (Exception e) {
            // expected
        }

        // drop
        try {
            connection1.createStatement().execute(String.format("drop view %s.%s", tableSchema.schemaName, VIEW_NAME_1));
            System.out.println("Done dropping.");
        } catch (SQLException e) {
            e.printStackTrace(System.err);
            Assert.fail(e.getLocalizedMessage());
        }
        connection1.commit();

        // good query here, but view is gone
        try {
            connection1.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
            Assert.fail("Expected an exception but didn't get one.");
        } catch (Exception e) {
            // expected
        }
    }
}
