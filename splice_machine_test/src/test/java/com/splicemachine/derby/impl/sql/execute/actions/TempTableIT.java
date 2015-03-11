package com.splicemachine.derby.impl.sql.execute.actions;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SQLClosures;
import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.utils.SpliceUtilities;

/**
 * @author Jeff Cunningham
 *         Date: 1/25/15
 */
public class TempTableIT {
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
            new SpliceIndexWatcher(CONSTRAINT_TEMP_TABLE,CLASS_NAME, "IDX_TEMP",tableSchema.schemaName,"(id)",true).starting(null);

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
                                                        tableSchema.schemaName,  EMP_NAME_PRIV_VIEW) + viewDef);
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
     * Make sure the HBase table that backs a Splice temp table gets cleaned up at the end of the user session.
     * @throws Exception
     */
    @Test
    public void testTempHBaseTableGetsDropped() throws Exception {
        long start = System.currentTimeMillis();
        HBaseAdmin hBaseAdmin = SpliceUtilities.getAdmin();
        String tempConglomID;
        boolean hbaseTempExists;
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
            tempConglomID = TestUtils.lookupConglomerateNumber(tableSchema.schemaName, SIMPLE_TEMP_TABLE, methodWatcher);
            hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
            Assert.assertTrue("HBase temp table ["+tempConglomID+"] does not exist.", hbaseTempExists);
        }  finally {
            methodWatcher.closeAll();
        }
        hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        if (hbaseTempExists) {
            // HACK: wait a sec, try again.  It's going away, just takes some time.
            Thread.sleep(2000);
            hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        }
        Assert.assertFalse("HBase temp table [" + tempConglomID + "] still exists.", hbaseTempExists);
        System.out.println("HBase Table check took: "+TestUtils.getDuration(start, System.currentTimeMillis()));
    }
}
