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

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 6/6/13
 */
public class ViewConstantOperationIT extends SpliceUnitTest { 
    public static final String CLASS_NAME = ViewConstantOperationIT.class.getSimpleName().toUpperCase();

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
        connection1.createStatement().execute(
                String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
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
     * Basic view create / drop with view in different schema than tables.
     * @throws Exception
     */
    @Test
    public void testViewCreateDropDiffSchema() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        // create
        connection1.createStatement().execute(String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                viewSchema.schemaName,
                VIEW_NAME_2,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE)));
        ResultSet resultSet = connection1.createStatement().executeQuery(String.format("select * from %s.%s", viewSchema.schemaName, VIEW_NAME_2));
        Assert.assertEquals(5, resultSetSize(resultSet));

        // drop
        connection1.createStatement().execute(String.format("drop view %s.%s", viewSchema.schemaName, VIEW_NAME_2));
        connection1.commit();

        // attempt read
        try {
            resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", VIEW_NAME_2));
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
            connection1.createStatement().executeQuery(String.format("select lname from %s.%s", tableSchema.schemaName, VIEW_NAME_1));
            Assert.fail("Expected an exception but didn't get one.");
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * Test view creation isolation.
     * @throws Exception
     */
    @Test
    public void testViewCreationIsolation() throws Exception {
        String create =  String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                tableSchema.schemaName,
                VIEW_NAME_1,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE));
        String query = String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1);

        Connection connection1 = methodWatcher.createConnection();
        Statement statement1 = connection1.createStatement();
        Connection connection2 = methodWatcher.createConnection();
        try {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            statement1.execute(create);

            ResultSet resultSet;
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Access to non-existing view didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            resultSet = connection1.createStatement().executeQuery(query);
            Assert.assertTrue("Connection should see its own writes",resultSet.next());

            connection1.commit();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Access to non-existing view didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            connection2.commit();
            resultSet = connection2.createStatement().executeQuery(query);
            Assert.assertTrue("New Transaction cannot see created object",resultSet.next());
        } finally {
            // drop/delete the damn thing
            try {
                connection1.createStatement().execute(String.format("drop view %s.%s", tableSchema.schemaName, VIEW_NAME_1));
                connection1.commit();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Test view rollback isolation.
     * @throws Exception
     */
    @Test
    public void testViewRollbackIsolation() throws Exception {
        String create =  String.format("create view %s.%s (id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id",
                tableSchema.schemaName,
                VIEW_NAME_1,
                this.getTableReference(EMP_NAME_TABLE),
                this.getTableReference(EMP_PRIV_TABLE));
        String query = String.format("select * from %s.%s", tableSchema.schemaName, VIEW_NAME_1);

        Connection connection1 = methodWatcher.createConnection();
        Statement statement1 = connection1.createStatement();
        Connection connection2 = methodWatcher.createConnection();
        try {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            statement1.execute(create);

            ResultSet resultSet;
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Access to non-existing view didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            resultSet = connection1.createStatement().executeQuery(query);
            Assert.assertTrue("Connection should see its own writes",resultSet.next());

            connection1.rollback();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Access to non committed view didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }

            connection2.commit();
            try {
                resultSet = connection2.createStatement().executeQuery(query);
                Assert.fail("Access to non committed view didn't raise exception");
            } catch (SQLException e) {
                Assert.assertTrue("Unknown exception", e.getMessage().contains("does not exist"));
            }
        } finally {
            // drop/delete the damn thing
            try {
                connection1.createStatement().execute(String.format("drop view %s.%s", tableSchema.schemaName, VIEW_NAME_1));
                connection1.commit();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Test
    public void testCantCreateViewWithSameName() throws Exception {
        methodWatcher.getStatement().execute(String.format("create view %s.duplicateView as select * from %s",
                tableSchema.schemaName,
                this.getTableReference(EMP_NAME_TABLE)));
        try {
            methodWatcher.getStatement().execute(String.format("create view %s.duplicateView as select * from %s",
                    tableSchema.schemaName,
                    this.getTableReference(EMP_NAME_TABLE)));
            Assert.fail("Shouldn't create an index with a duplicate name");
        } catch (SQLException e) {
            Assert.assertTrue(e.getCause().getMessage().contains("already exists"));
        }
    }
}
