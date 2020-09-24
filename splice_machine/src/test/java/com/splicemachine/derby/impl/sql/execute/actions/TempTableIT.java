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
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Jeff Cunningham
 *         Date: 1/25/15
 */
public class TempTableIT extends SpliceUnitTest {
    public static final String CLASS_NAME = TempTableIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

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

    private static final String SIMPLE_TEMP_TABLE = "SIMPLE_TEMP_TABLE";
    private static String simpleDef = "(id int, fname varchar(8), lname varchar(10))";

    private static final String CONSTRAINT_TEMP_TABLE = "CONSTRAINT_TEMP_TABLE";
    private static String constraintTableDef = "(id int not null primary key, fname varchar(8) not null, lname varchar(10) not null)";

    private static final String EMP_PRIV_TABLE = "EMP_PRIV";
    private static String ePrivDef = "(id int not null primary key, dob varchar(10) not null, ssn varchar(12) not null)";
    private static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE,CLASS_NAME, ePrivDef);

    private static final String CONSTRAINT_TEMP_TABLE1 = "CONSTRAINT_TEMP_TABLE1";
    private static SpliceTableWatcher constraintTable1 = new SpliceTableWatcher(CONSTRAINT_TEMP_TABLE1,CLASS_NAME, constraintTableDef);
    private static final String EMP_NAME_PRIV_VIEW = "EMP_VIEW";
    private static String viewFormat = "(id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id";
    private static String viewDef = String.format(viewFormat, constraintTable1.toString(), empPrivTable.toString());

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(tableSchema)
                                            .around(empPrivTable);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    // ===============================================================
    // Test Helpers
    // ===============================================================

    /**
     * Help test temp table syntax parsing.
     * @throws Exception
     */
    private void helpTestSyntax(final String sqlString, final String expectedExceptionMsg) throws Exception {
        Connection connection = methodWatcher.createConnection();
        boolean expectExcepiton = (expectedExceptionMsg != null && ! expectedExceptionMsg.isEmpty());
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(sqlString, tableSchema.schemaName, SIMPLE_TEMP_TABLE,
                                                    simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });
            connection.commit();
            if (expectExcepiton) {
                fail("Expected exception '"+expectedExceptionMsg+"' but didn't get one.");
            }
        } catch (Exception e) {
            if (! expectExcepiton) {
                throw e;
            }
            assertEquals("Expected exception '"+expectedExceptionMsg+"' but got: "+e.getLocalizedMessage(),
                         expectedExceptionMsg, e.getLocalizedMessage());
        } finally {
            connection = methodWatcher.createConnection();
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table if exists %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                }
            });
            methodWatcher.closeAll();
        }
    }

    // ===============================================================
    // Tests
    // ===============================================================

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     *
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerby() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "NOT LOGGED"
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerbyNotLogged() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s not logged";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s not logged";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "NOLOGGING"
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerbyNoLogging() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s nologging";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s nologging";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s nologging";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "ON COMMIT PRESERVE ROWS"
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerbyOnCommitPreserveRows() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "ON COMMIT DELETE ROWS"
     * ON COMMIT DELTE ROWS is unsupported
     * @throws Exception
     */
    @Test(expected=SQLException.class)
    public void testCreateTempTableDerbyOnCommitDeleteRows() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s ON COMMIT DELETE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT DELETE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s ON COMMIT DELETE ROWS";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "ON ROLLBACK PRESERVE ROWS"
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerbyOnRollbackPreserveRows() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s ON ROLLBACK PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON ROLLBACK PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s ON ROLLBACK PRESERVE ROWS";
        helpTestSyntax(tmpCreate, null);
    }

    /**
     * Test given syntax of all three supported; Derby, Tableau and MicroStrategy
     * "ON ROLLBACK DELETE ROWS"
     * @throws Exception
     */
    @Test
    public void testCreateTempTableDerbyOnRollbackDeleteRows() throws Exception {
        String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s ON ROLLBACK DELETE ROWS";
        helpTestSyntax(tmpCreate, "DELETE ROWS is not supported for ON 'ROLLBACK'.");
        tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON ROLLBACK DELETE ROWS";
        helpTestSyntax(tmpCreate, "DELETE ROWS is not supported for ON 'ROLLBACK'.");
        tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s ON ROLLBACK DELETE ROWS";
        helpTestSyntax(tmpCreate, "DELETE ROWS is not supported for ON 'ROLLBACK'.");
    }

    /**
     * Create/drop temp table in Derby syntax with ON ROLLBACK DELETE ROWS.
     *
     * @throws Exception
     */
    @Ignore("ON ROLLBACK DELETE ROWS is unsupported")
    @Test
    public void testCreateTempTableRollback() throws Exception {
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s on rollback delete rows";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("insert into %s values (006,'Fred','Ziffle')", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                }
            });
            connection.rollback();

            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(0, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });
            connection.commit();
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table in Derby syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableSimpleDerby() throws Exception {
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged on commit preserve rows";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName,
                                                        SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            SIMPLE_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        }  finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table in Derby syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropCreateTempTableSimpleDerby() throws Exception {
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged on commit preserve rows";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName,
                                                        SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            SIMPLE_TEMP_TABLE));
                        Assert.fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
        }  finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table with constraints defined in Derby syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableWithConstraintsDerby() throws Exception {
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged on commit preserve rows";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE,
                                             empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName,
                                                        CONSTRAINT_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            CONSTRAINT_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table in Tableau syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableSimpleTableau() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            SIMPLE_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table with constraints defined in Tableau syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableWithConstraintsTableau() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE,
                                             empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName,
                                                        CONSTRAINT_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            CONSTRAINT_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table with constraints defined in MicroStrategy syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableWithConstraintsMicroStrategy() throws Exception {
        final String tmpCreate = "CREATE GLOBAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE,
                                             empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection, String.format("select * from %s.%s", tableSchema.schemaName,
                                                        CONSTRAINT_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." +
                            CONSTRAINT_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create/drop temp table with index defined in Tableau syntax.
     *
     * @throws Exception
     */
    @Test
    public void testCreateDropTempTableWithIndexTableau() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE,
                                             empNameVals);
                }
            });
            connection.commit();

            // Create index
            try {
                new SpliceIndexWatcher(CONSTRAINT_TEMP_TABLE, CLASS_NAME, "IDX_TEMP", tableSchema.schemaName, "(id)", true).starting(null);
                fail("Expected exception invalid create index statement since local temporary tables are not visible to other sessions.");
            } catch (RuntimeException e) {
                // expected
            }
            SpliceIndexWatcher.createIndex(connection, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE, "IDX_TEMP", "(id)", true, false, false);

            SQLClosures.query(connection, String.format("select id from %s.%s", tableSchema.schemaName,
                                                        CONSTRAINT_TEMP_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                                  }
                              });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    try {
                        statement.executeQuery(String.format("select id from %s", tableSchema.schemaName + "." +
                            CONSTRAINT_TEMP_TABLE));
                        fail("Expected exception querying temp table that no longer should exist.");
                    } catch (SQLException e) {
                        // expected
                    }
                }
            });
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Attempt to create a table with a foreign key referencing a primary key in a temp table.
     * Should fail.
     *
     * @throws Exception
     */
    @Test
    public void testCreateTableWithForeignKeyPointingToTempTable() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection = methodWatcher.createConnection();
        try {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE,
                                             empNameVals);
                }
            });
            connection.commit();

            // Create foreign key
            final String tableWithFK = "create table %s.%s (a int, a_id int CONSTRAINT id_fk REFERENCES %s.%s(id))";
            try {
                SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                    @Override
                    public void execute(Statement statement) throws Exception {
                        statement.execute(String.format(tableWithFK,
                                                        tableSchema.schemaName, "TABLE_WITH_FOREIGN_KEY",
                                                        tableSchema.schemaName, CONSTRAINT_TEMP_TABLE));
                    }
                });
                fail("Expected an exception attempting to create a table with a foreign key pointing to a temp table " +
                         "column.");
            } catch (SQLSyntaxErrorException e) {
                Assert.assertEquals("Temporary table columns cannot be referenced by foreign keys.", e.getLocalizedMessage());
            }
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Create view that uses a temp table for provider.  Should fail.
     *
     * @throws Exception
     */
    @Test
    public void testCreateViewWithTempTable() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + EMP_PRIV_TABLE, empPrivVals);
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, CONSTRAINT_TEMP_TABLE1,
                                                    constraintTableDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + CONSTRAINT_TEMP_TABLE1,
                                             empNameVals);
                }
            });
            connection.commit();

            try {
                SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                    @Override
                    public void execute(Statement statement) throws Exception {
                        statement.execute(String.format("create view %s.%s ",
                                                        tableSchema.schemaName, EMP_NAME_PRIV_VIEW) + viewDef);
                        fail("Expected exception trying to create a view that depends on a temp table.");
                    }
                });
            } catch (Exception e) {
                // expected
                Assert.assertTrue(e.getLocalizedMessage().startsWith("Attempt to add temporary table"));
            }
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Make sure a temp table is deleted after the end of the user session.
     * @throws Exception
     */
    @Test
    public void testTempTableGetsDropped() throws Exception {
        // DB-3769: temp table is accessible after session termination
        final String MY_TABLE = "MY_TABLE";
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        Connection connection1 = methodWatcher.createConnection();
        SQLClosures.execute(connection1, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, MY_TABLE,
                                                    ePrivDef));
                SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + MY_TABLE, empPrivVals);
            }
        });
        connection1.commit();

        SQLClosures.query(connection1, String.format("select id from %s.%s", tableSchema.schemaName, MY_TABLE),
                          new SQLClosures.SQLAction<ResultSet>() {
                              @Override
                              public void execute(ResultSet resultSet) throws Exception {
                                  Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet));
                              }
                          });
        connection1.commit();
        methodWatcher.closeAll();

        Thread.sleep(5000);  // TODO: JC - This is what the bug is about now. We have to wait for table drop before connecting.
        Connection connection2 = methodWatcher.createConnection();
        try {
            SQLClosures.query(connection2, String.format("select id from %s.%s", tableSchema.schemaName, MY_TABLE),
                              new SQLClosures.SQLAction<ResultSet>() {
                                  @Override
                                  public void execute(ResultSet resultSet) throws Exception {
                                      // expecting exception to be thrown from query
                                      fail("Expected TEMP table to be gone.");
                                  }
                              });
        } catch (SQLException e) {
            e.printStackTrace();
            // expected
            Assert.assertEquals(e.getLocalizedMessage(),"42X05", e.getSQLState());
        }
        connection2.commit();
    }

    /**
     * Test session visibility of local temporary tables.
     *
     * @throws Exception
     */
    @Test
    public void testLocalTempTableSessionVisibility() throws Exception {
        final String createStmt = String.format("CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS",
                tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef);
        final String selectStmt = String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE);

        Connection connection_1 = methodWatcher.createConnection();
        Connection connection_2 = methodWatcher.createConnection();
        try {
            // create a local temporary table in connection_1 and load some data into it
            SQLClosures.execute(connection_1, statement -> {
                statement.execute(createStmt);
                // insert in the same session
                SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
            });
            connection_1.commit();

            // select in the same session should success
            SQLClosures.query(connection_1, selectStmt,
                    resultSet -> Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet)));

            // try operating on the local temporary table from connection_2
            SQLClosures.execute(connection_2, statement -> {
                // select should fail
                // Note that this select uses exactly the same SQL string as the one above. It must fail or session
                // visibility is broken.
                try {
                    statement.executeQuery(selectStmt);
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42X05", e.getSQLState());
                }

                // insert should fail
                try {
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42X05", e.getSQLState());
                }

                // update should fail
                try {
                    statement.execute(String.format("update %s set id = 2", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42X05", e.getSQLState());
                }

                // delete should fail
                try {
                    statement.execute(String.format("delete from %s where id = 2", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42X05", e.getSQLState());
                }

                // drop should fail
                try {
                    statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42Y55", e.getSQLState());
                }

                // create index should fail
                try {
                    statement.execute(String.format("create index temp_idx1 on %s(fname)", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"42Y55", e.getSQLState());
                }

                // system procedure should fail
                try {
                    statement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", tableSchema.schemaName, SIMPLE_TEMP_TABLE));
                    fail("Expected exception of table doesn't exist since it's not visible to this session.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"XIE0M", e.getSQLState());
                }
            });

            // now create a local temporary table with the same name in connection_2
            // this DDL is the same SQL string as connection_1 uses, but no data is loaded
            SQLClosures.execute(connection_2, statement -> { statement.execute(createStmt); });
            connection_2.commit();

            // select in the same session should success
            // since connection_2 sees only its own local temporary tables, the table should be empty
            SQLClosures.query(connection_2, selectStmt,
                    resultSet -> Assert.assertEquals(0, SpliceUnitTest.resultSetSize(resultSet)));

            // do the same select in connection_1, it should get 5 rows instead
            SQLClosures.query(connection_1, selectStmt,
                    resultSet -> Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet)));
        } finally {
            // both tables are destroyed
            methodWatcher.closeAll();
        }
    }

    /**
     * Test session visibility of indexes on local temporary tables.
     *
     * @throws Exception
     */
    @Test
    public void testLocalTempTableIndexesSessionVisibility() throws Exception {
        final String createTblStmt = String.format("CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS",
                tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef);
        final String createIdxPattern = "CREATE INDEX %s ON %s.%s(fname)";
        final String dropIdxPattern = "DROP INDEX %s";
        final String idxName = "TEMP_IDX_00001";
        final String selectStmt = String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE);

        Connection connection_1 = methodWatcher.createConnection();
        Connection connection_2 = methodWatcher.createConnection();
        try {
            // create a local temporary table in connection_1, load some data, and create an index TEMP_IDX_00001
            SQLClosures.execute(connection_1, statement -> {
                statement.execute(createTblStmt);
                // insert in the same session
                SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
            });
            SpliceIndexWatcher.createIndex(connection_1, tableSchema.schemaName, SIMPLE_TEMP_TABLE, idxName, "(fname)", true, false, false);
            connection_1.commit();

            // select in the same session should success
            SQLClosures.query(connection_1, selectStmt,
                    resultSet -> Assert.assertEquals(5, SpliceUnitTest.resultSetSize(resultSet)));

            // drop index TEMP_IDX_00001 from connection_2 should fail
            try {
                SpliceIndexWatcher.executeDrop(connection_2, tableSchema.schemaName, idxName);
                fail("Expected exception of table doesn't exist since it's not visible to this session.");
            } catch (RuntimeException e) {
                // expected
                Assert.assertTrue(e.getLocalizedMessage().contains(SIMPLE_TEMP_TABLE));
                Assert.assertFalse(e.getLocalizedMessage().contains(idxName));
            }

            // now create a local temporary table in connection_2
            SQLClosures.execute(connection_2, statement -> { statement.execute(createTblStmt); });
            connection_2.commit();

            // connection_2 cannot create an index with the same name because index names are not mangled
            // so connection_2 cannot create this index name, nor can it drop it, just choose another name
            SQLClosures.execute(connection_2, statement -> {
                try {
                    statement.execute(String.format(createIdxPattern, idxName, tableSchema.schemaName, SIMPLE_TEMP_TABLE));
                    fail("Expected exception of index already exist since index names are not mangled.");
                } catch (SQLException e) {
                    // expected
                    Assert.assertEquals(e.getLocalizedMessage(),"X0Y32", e.getSQLState());
                    Assert.assertTrue(e.getLocalizedMessage().contains(idxName));
                }
            });

            // drop index TEMP_IDX_00001 from connection_1 should be fine
            SpliceIndexWatcher.executeDrop(connection_1, tableSchema.schemaName, idxName);
        } finally {
            // both tables are destroyed
            methodWatcher.closeAll();
        }
    }

    /**
     * Test ALTER TABLE and RENAME TABLE on temporary tables. Should fail.
     *
     * @throws Exception
     */
    @Test
    public void testAlterAndRenameTableOnTempTable() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection, statement -> {
                statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
            });
            connection.commit();

            try {
                SQLClosures.execute(connection, statement -> {
                    statement.execute(String.format("alter table %s.%s add column xyz int",
                            tableSchema.schemaName, SIMPLE_TEMP_TABLE));
                    fail("Expected exception since ALTER TABLE is not allowed on a temp table.");
                });
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "42995", e.getSQLState());
            }

            try {
                SQLClosures.execute(connection, statement -> {
                    statement.execute(String.format("rename table %s.%s to xyz",
                            tableSchema.schemaName, SIMPLE_TEMP_TABLE));
                    fail("Expected exception since RENAME TABLE is not allowed on a temp table.");
                });
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "42995", e.getSQLState());
            }
        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Test system procedures on temporary tables.
     * Only those applicable to a table are tested.
     *
     * @throws Exception
     */
    @Test
    public void testSystemProceduresOnTempTable() throws Exception {
        final String tmpCreate = "CREATE LOCAL TEMPORARY TABLE %s.%s %s ON COMMIT PRESERVE ROWS";
        final String tableName = "A";
        try (Connection connection = methodWatcher.createConnection()) {
            // create a local temporary table
            SQLClosures.execute(connection, statement -> {
                statement.execute(String.format(tmpCreate, tableSchema.schemaName, tableName, "(name varchar(40) primary key, title varchar(40), age int)"));
            });
            connection.commit();

            // import data
            final File BADDIR = SpliceUnitTest.createBadLogDirectory(tableSchema.schemaName);
            SQLClosures.execute(connection, statement -> {
                statement.execute(String.format("call syscs_util.import_data('%s', '%s', null, '%s', ',', null, null, null, null, 0, '%s', null, null)",
                        tableSchema.schemaName, tableName, getResourceDirectory() + "importTest.in", BADDIR.getCanonicalPath()));
            });
            connection.commit();

            // merge data
            // since NAME is a PK, merge should do upsert, which means table content is not changed
            SQLClosures.execute(connection, statement -> {
                statement.execute(String.format("call syscs_util.merge_data_from_file('%s', '%s', null, '%s', ',', null, null, null, null, 0, '%s', null, null)",
                        tableSchema.schemaName, tableName, getResourceDirectory() + "importTest.in", BADDIR.getCanonicalPath()));
            });
            connection.commit();

            try {
                SQLClosures.execute(connection, statement -> {
                    statement.execute(String.format("call syscs_util.snapshot_table('%s', '%s', '%s_snapshot_1')",
                            tableSchema.schemaName, tableName, tableName));
                    fail("Expected exception since SNAPSHOT_TABLE is not allowed on a temp table.");
                });
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "42995", e.getSQLState());
            }

            SQLClosures.execute(connection, statement -> {
                statement.execute(String.format("call syscs_util.enable_column_statistics('%s', '%s', 'title')",
                        tableSchema.schemaName, tableName));
                statement.execute(String.format("call syscs_util.disable_column_statistics('%s', '%s', 'title')",
                        tableSchema.schemaName, tableName));
                statement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')",
                        tableSchema.schemaName, tableName));
                statement.execute(String.format("call syscs_util.syscs_perform_major_compaction_on_table('%s', '%s')",
                        tableSchema.schemaName, tableName));
                statement.execute(String.format("call syscs_util.set_purge_deleted_rows('%s', '%s', 'true')",
                        tableSchema.schemaName, tableName));
                statement.execute(String.format("call syscs_util.show_create_table('%s', '%s')",
                        tableSchema.schemaName, tableName));
            });

            String[] encodedRegion = { null };
            SQLClosures.query(connection, String.format("call syscs_util.get_encoded_region_name('%s', '%s', null, '1', '|', null, null, null, null)",
                    tableSchema.schemaName, tableName),
                    resultSet -> {
                        if (resultSet.next())
                            encodedRegion[0] = resultSet.getString(1);
                        if (resultSet.next())
                            fail("There should be only one encoded region for table" + tableSchema.schemaName + "." + tableName);
                    }
            );

            if (encodedRegion[0] != null) {
                SQLClosures.execute(connection, statement -> {
                    statement.execute(String.format("call syscs_util.compact_region('%s', '%s', null, '%s')",
                            tableSchema.schemaName, tableName, encodedRegion[0]));
                    statement.execute(String.format("call syscs_util.major_compact_region('%s', '%s', null, '%s')",
                            tableSchema.schemaName, tableName, encodedRegion[0]));
                    statement.execute(String.format("call syscs_util.get_start_key('%s', '%s', null, '%s')",
                            tableSchema.schemaName, tableName, encodedRegion[0]));
                });
            }

        } finally {
            methodWatcher.closeAll();
        }
    }

    /**
     * Test name clash rules of local temporary tables.
     *
     * @throws Exception
     */
    @Test
    public void testLocalTempTableNameClash() throws Exception {
        final String createTablePattern = "CREATE %s " + String.format("TABLE %s.%s %s", tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef);
        final String selectStmt = String.format("select * from %s.%s", tableSchema.schemaName, SIMPLE_TEMP_TABLE);

        Connection connection_1 = methodWatcher.createConnection();
        Connection connection_2 = methodWatcher.createConnection();
        try {
            // create a local temporary table in connection_1
            SQLClosures.execute(connection_1, statement -> {
                statement.execute(String.format(createTablePattern, "LOCAL TEMPORARY"));
            });
            connection_1.commit();

            // create another local temporary table with the same name in connection_1, should fail
            try {
                SQLClosures.execute(connection_1, statement -> {
                    statement.execute(String.format(createTablePattern, "LOCAL TEMPORARY"));
                });
                fail("Expected exception of table already exists.");
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "X0Y32", e.getSQLState());
            }

            // local temporary names clash with base tables globally
            // create base table with the same name in connection_1, should fail
            try {
                SQLClosures.execute(connection_1, statement -> {
                    statement.execute(String.format(createTablePattern, ""));
                });
                fail("Expected exception of table already exists.");
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "X0Y32", e.getSQLState());
            }

            // create base table with the same name in connection_2, should fail
            try {

                SQLClosures.execute(connection_2, statement -> {
                    statement.execute(String.format(createTablePattern, ""));
                });
                fail("Expected exception of table name clashes with a local temporary table.");
            } catch (SQLException e) {
                // expected
                Assert.assertEquals(e.getLocalizedMessage(), "42ZD4", e.getSQLState());
            }

            // create a local temporary table with the same name in connection_2, OK
            SQLClosures.execute(connection_2, statement -> {
                statement.execute(String.format(createTablePattern, "LOCAL TEMPORARY"));
            });

            // drop tables are not part of the test but just to make sure TEMPTABLEIT schema can be dropped
            SQLClosures.execute(connection_1, statement -> {
                statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
            });
            SQLClosures.execute(connection_2, statement -> {
                statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE));
            });
        } finally {
            // both tables are destroyed
            methodWatcher.closeAll();
        }
    }
}
