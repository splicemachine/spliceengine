package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceViewWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Jeff Cunningham
 *         Date: 6/7/13
 */
public class TableConstantOperationIT extends SpliceUnitTest { 
    public static final String CLASS_NAME = TableConstantOperationIT.class.getSimpleName().toUpperCase();

    private static final List<String> empNameVals = Arrays.asList(
            "(001,'Jeff','Cunningham')",
            "(002,'Bill','Gates')",
            "(003,'John','Jones')",
            "(004,'Warren','Buffet')",
            "(005,'Tom','Jones')");

    private static final List<String> empPrivVals = Arrays.asList(
            "(001,'04/08/1900','555-123-4567')",
            "(002,'02/20/1999','555-123-4577')",
            "(003,'11/31/2001','555-123-4587')",
            "(004,'06/05/1985','555-123-4597')",
            "(005,'09/19/1968','555-123-4507')");

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    public static final String EMP_NAME_TABLE1 = "emp_name1";
    public static final String EMP_NAME_TABLE2 = "emp_name2";
    public static final String EMP_NAME_TABLE3 = "emp_name3";
    public static final String EMP_NAME_TABLE4 = "emp_name4";
    public static final String EMP_NAME_TABLE5 = "emp_name5";

    public static final String EMP_PRIV_TABLE1 = "emp_priv1";
    public static final String EMP_PRIV_TABLE2 = "emp_priv2";
    public static final String EMP_PRIV_TABLE3 = "emp_priv3";

    public static final String EMP_NAME_PRIV_VIEW = "emp_name_priv";
    public static final String EMP_NAME_PRIV_VIEW2 = "emp_name_priv2";

    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

    private static String eNameDef = "(id int not null primary key, fname varchar(8) not null, lname varchar(10) not null)";
    private static String ePrivDef = "(id int not null primary key, dob varchar(10) not null, ssn varchar(12) not null)";
    protected static SpliceTableWatcher empNameTable = new SpliceTableWatcher(EMP_NAME_TABLE1,CLASS_NAME, eNameDef);
    protected static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE1,CLASS_NAME, ePrivDef);
    protected static SpliceTableWatcher empPrivTable2 = new SpliceTableWatcher(EMP_PRIV_TABLE2,CLASS_NAME, ePrivDef);
    protected static SpliceTableWatcher empPrivTable3 = new SpliceTableWatcher(EMP_PRIV_TABLE3,CLASS_NAME, ePrivDef);

    private static String viewFormat = "(id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id";
    private static String viewDef = String.format(viewFormat, empNameTable.toString(), empPrivTable2.toString());
    private static String viewDef2 = String.format(viewFormat, empNameTable.toString(), empPrivTable3.toString());
    protected static SpliceViewWatcher empNamePrivView = new SpliceViewWatcher(EMP_NAME_PRIV_VIEW,CLASS_NAME, viewDef);
    protected static SpliceViewWatcher empNamePrivView2 = new SpliceViewWatcher(EMP_NAME_PRIV_VIEW2,CLASS_NAME, viewDef2);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(empNameTable)
            .around(empPrivTable)
            .around(empPrivTable2)
            .around(empPrivTable3)
            .around(empNamePrivView)
            .around(empNamePrivView2)
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

                        //  load emp_priv (view) table
                        for (String rowVal : empPrivVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empPrivTable2.toString() + " values " + rowVal);
                        }

                        //  load emp_priv2 (view) table
                        for (String rowVal : empPrivVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empPrivTable3.toString() + " values " + rowVal);
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
     * Load a table with given values
     *
     * @param statement calling test's statement that may be in txn
     * @param tableName fully-qualified table name, i.e., <pre>schema.table</pre>
     * @param values list of row values
     * @throws Exception
     */
    public static void loadTable(Statement statement, String tableName, List<String> values) throws Exception {
        for (String rowVal : values) {
            statement.executeUpdate("insert into " + tableName + " values " + rowVal);
        }
    }

    @Test
    public void testCreateDropTable() throws Exception {
        Connection connection = methodWatcher.createConnection();
        Statement statement = methodWatcher.createConnection().createStatement();
        statement.execute(String.format("create table %s.%s %s",tableSchema.schemaName,EMP_NAME_TABLE2,eNameDef));
        loadTable(statement, tableSchema.schemaName + "." + EMP_NAME_TABLE2, empNameVals);
        connection.commit();
        ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, EMP_NAME_TABLE2));
        Assert.assertEquals(5, resultSetSize(resultSet));

        statement.execute(String.format("drop table %s",tableSchema.schemaName + "." + EMP_NAME_TABLE2));
        connection.commit();

        try {
            connection.createStatement().executeQuery(String.format("select * from %s", tableSchema.schemaName + "." + EMP_NAME_TABLE2));
            Assert.fail("Expected exception but didn't get one.");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    public void testCreateDropTableIfExist() throws Exception {
        String tableName = "R";
        methodWatcher.getStatement().execute(String.format("create table %s.%s (i int)", tableSchema.schemaName, tableName));
        methodWatcher.getStatement().execute(String.format("drop table %s", tableSchema.schemaName + "." + tableName));

        try {
            methodWatcher.getStatement().execute(String.format("drop table %s", tableSchema.schemaName + "." + tableName));
            Assert.fail("Expected exception but didn't get one.");
        } catch (SQLException e) {
            // expected - no "if exists"
        }

        // we should not get an exception here because we've used "if exists"
        methodWatcher.getStatement().execute(String.format("drop if exists table %s", tableSchema.schemaName + "." + tableName));
    }

    @Test
    public void testRenameTable() throws Exception {
        Connection connection = methodWatcher.createConnection();
        Statement statement = methodWatcher.createConnection().createStatement();
        ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, EMP_PRIV_TABLE1));
        Assert.assertEquals(5, resultSetSize(resultSet));

        statement.execute(String.format("rename table %s.%s to %s",tableSchema.schemaName,EMP_PRIV_TABLE1,"real_private"));
        connection.commit();

        try {
            connection.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, EMP_PRIV_TABLE1));
            Assert.fail("Expected exception but didn't get one.");
        } catch (SQLException e) {
            // expected
        }

        resultSet = connection.createStatement().executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, "real_private"));
        Assert.assertEquals(5, resultSetSize(resultSet));
    }

    @Test
    public void testDropTableWithView() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        connection.createStatement().execute(String.format("delete from %s where id = 1", empPrivTable3.toString()));
        ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s", empNamePrivView2.toString()));
        Assert.assertEquals(4, resultSetSize(resultSet));

        try {
            connection.createStatement().execute(String.format("drop table %s.%s", tableSchema.schemaName, EMP_PRIV_TABLE3));
            Assert.fail("Expected exception but didn't get one.");
        } catch (SQLException e) {
            // expected
        }

        resultSet = connection.createStatement().executeQuery(String.format("select * from %s", empNamePrivView2.toString()));
        Assert.assertEquals(4, resultSetSize(resultSet));
        connection.commit();
    }

    @Test
     public void testRenameTableWithView() throws Exception {
        Connection connection = methodWatcher.createConnection();
        ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s", empNamePrivView.toString()));
        Assert.assertEquals(5, resultSetSize(resultSet));

        try {
            connection.createStatement().execute(String.format("rename table %s.%s to %s", tableSchema.schemaName, EMP_PRIV_TABLE2, "real_private"));
            Assert.fail("Expected exception but didn't get one.");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    public void testTableCreationIsolation() throws Exception {
        String create = String.format("create table %s.%s %s",tableSchema.schemaName,EMP_NAME_TABLE3,ePrivDef);
        String query = String.format("select * from %s.%s", tableSchema.schemaName, EMP_NAME_TABLE3);

        Connection connection1 = methodWatcher.createConnection();
        Statement statement1 = connection1.createStatement();
        Connection connection2 = methodWatcher.createConnection();
        try {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            statement1.execute(create);
            loadTable(statement1, tableSchema.schemaName + "." + EMP_NAME_TABLE3, empNameVals);

            ResultSet resultSet;
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to non committed table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            resultSet = connection1.createStatement().executeQuery(query);
            Assert.assertTrue("Connection should see its own writes",resultSet.next());

            connection1.commit();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to not-in-snapshot table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            connection2.commit();
            resultSet = connection2.createStatement().executeQuery(query);
            Assert.assertTrue("New Transaction cannot see created object",resultSet.next());
        } finally {
            // drop/delete the damn thing
            try {
                connection1.createStatement().execute(String.format("drop table %s.%s", tableSchema.schemaName, EMP_NAME_TABLE3));
                connection1.commit();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Test
    public void testTableDropIsolation() throws Exception {
        String create = String.format("create table %s.%s %s",tableSchema.schemaName,EMP_NAME_TABLE4,ePrivDef);
        String query = String.format("select * from %s.%s", tableSchema.schemaName, EMP_NAME_TABLE4);

        Connection connection1 = methodWatcher.createConnection();
        Statement statement1 = connection1.createStatement();
        Connection connection2 = methodWatcher.createConnection();
        try {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            statement1.execute(create);
            loadTable(statement1, tableSchema.schemaName + "." + EMP_NAME_TABLE4, empNameVals);

            ResultSet resultSet;
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to non committed table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            resultSet = connection1.createStatement().executeQuery(query);
            Assert.assertTrue("Connection should see its own writes",resultSet.next());

            connection1.rollback();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to non committed table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            connection2.commit();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to non committed table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }
        } finally {
            // drop/delete the damn thing
            try {
                connection1.createStatement().execute(String.format("drop table %s.%s", tableSchema.schemaName, EMP_NAME_TABLE4));
                connection1.commit();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Test
    public void testTableCreationConflict() throws Exception {
        String create = String.format("create table %s.%s %s",tableSchema.schemaName,EMP_NAME_TABLE5,ePrivDef);
        String query = String.format("select * from %s.%s", tableSchema.schemaName, EMP_NAME_TABLE5);

        Connection connection1 = methodWatcher.createConnection();
        Connection connection2 = methodWatcher.createConnection();
        try {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            connection1.createStatement().execute(create);
            loadTable(connection1.createStatement(), tableSchema.schemaName + "." + EMP_NAME_TABLE5, empNameVals);

            ResultSet resultSet;
            try {
                connection2.createStatement().execute(create);
                Assert.fail("Isolation violated, write conflict undetected");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getCause().getMessage().contains("serializable"));
            }
            connection2.rollback();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Read committed violated, access to not-in-snapshot table didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            resultSet = connection1.createStatement().executeQuery(query);
            Assert.assertTrue("Connection should see its own writes",resultSet.next());

            connection1.rollback();

            connection2.createStatement().execute(create);
            loadTable(connection2.createStatement(), tableSchema.schemaName + "." + EMP_NAME_TABLE5, empNameVals);
            connection2.commit();
            resultSet = connection2.createStatement().executeQuery(query);
            Assert.assertTrue("New Transaction cannot see created object",resultSet.next());
        } finally {
            // drop/delete the damn thing
            try {
                connection1.createStatement().execute(String.format("drop table %s.%s", tableSchema.schemaName, EMP_NAME_TABLE5));
                connection1.commit();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

}
