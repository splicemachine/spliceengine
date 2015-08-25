package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.pipeline.exception.ErrorState;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

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

    @Test(expected=SQLException.class)
    public void testCreateDropTable() throws Exception {
        Connection connection = methodWatcher.createConnection();
        try{
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("create table %s.%s %s", tableSchema.schemaName, EMP_NAME_TABLE2, eNameDef));
                    loadTable(statement, tableSchema.schemaName + "." + EMP_NAME_TABLE2, empNameVals);
                }
            });
            connection.commit();
            SQLClosures.query(connection,String.format("select * from %s.%s",tableSchema.schemaName,EMP_NAME_TABLE2),new SQLClosures.SQLAction<ResultSet>() {
                @Override
                public void execute(ResultSet resultSet) throws Exception {
                    Assert.assertEquals(5, resultSetSize(resultSet));
                }
            });

            SQLClosures.execute(connection,new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("drop table %s",tableSchema.schemaName + "." + EMP_NAME_TABLE2));
                }
            });
            connection.commit();

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.executeQuery(String.format("select * from %s", tableSchema.schemaName + "." + EMP_NAME_TABLE2));
                }
            });
        }finally{
            connection.close();
        }
    }

    @Test(expected=SQLException.class)
    public void testCannotCreateTableWithXmlColumn() throws Exception {
        Connection conn = methodWatcher.createConnection();
        conn.setAutoCommit(false);

        try {
            conn.createStatement().execute("create table testAlterTableXml (i XML)");
        } catch (SQLException se) {
            /*
             * The ErrorState.NOT_IMPLEMENTED ends with a .S, which won't be printed in the
             * error message, so we need to be sure that we strip it if it ends that way
             */
            String sqlState=ErrorState.NOT_IMPLEMENTED.getSqlState();
            int dotIdx = sqlState.indexOf(".");
            if(dotIdx>0)
                sqlState = sqlState.substring(0,dotIdx);
            Assert.assertEquals(sqlState,se.getSQLState());
            throw se;
        } finally {
            conn.rollback();
        }
    }

    @Test(expected = SQLException.class)
    public void testCreateDropTableIfExist() throws Exception {
        String tableName = "R";
        Statement statement = methodWatcher.getStatement();
        statement.execute(String.format("create table %s.%s (i int)", tableSchema.schemaName, tableName));
        statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + tableName));

        try {
            statement.execute(String.format("drop table %s", tableSchema.schemaName + "." + tableName));
        }finally{
            // we should not get an exception here because we've used "if exists"
            statement.execute(String.format("drop table if exists %s", tableSchema.schemaName + "." + tableName));
        }
    }

    @Test(expected = SQLException.class)
    public void testRenameTable() throws Exception {
        Connection connection = methodWatcher.createConnection();
        try{
            SQLClosures.query(connection,String.format("select * from %s.%s",tableSchema.schemaName,EMP_PRIV_TABLE1),new SQLClosures.SQLAction<ResultSet>() {
                @Override
                public void execute(ResultSet resultSet) throws Exception {
                    Assert.assertEquals(5, resultSetSize(resultSet));
                }
            });

            SQLClosures.execute(connection,new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("rename table %s.%s to %s",tableSchema.schemaName,EMP_PRIV_TABLE1,"real_private"));
                }
            });
            connection.commit();

            try{
                SQLClosures.execute(connection,new SQLClosures.SQLAction<Statement>() {
                    @Override
                    public void execute(Statement statement) throws Exception {
                        statement.executeQuery(String.format("select * from %s.%s", tableSchema.schemaName, EMP_PRIV_TABLE1));
                        Assert.fail("Expected exception but didn't get one.");
                    }
                });
            }finally{
                SQLClosures.query(connection,String.format("select * from %s.%s",tableSchema.schemaName,"real_private"),new SQLClosures.SQLAction<ResultSet>() {
                    @Override
                    public void execute(ResultSet resultSet) throws Exception {
                        Assert.assertEquals(5, resultSetSize(resultSet));
                    }
                });
            }
        }finally{
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void testDropTableWithView() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        try{
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("delete from %s where id = 1", empPrivTable3.toString()));
                }
            }); 
            SQLClosures.query(connection, String.format("select * from %s", empNamePrivView2.toString()), new SQLClosures.SQLAction<ResultSet>() {
                @Override
                public void execute(ResultSet resultSet) throws Exception {
                    Assert.assertEquals(4, resultSetSize(resultSet));
                }
            });

            try{
                SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                    @Override
                    public void execute(Statement statement) throws Exception {
                        statement.execute(String.format("drop table %s.%s", tableSchema.schemaName, EMP_PRIV_TABLE3));
                        Assert.fail("Expected exception but didn't get one.");
                    }
                });
            }finally{
                SQLClosures.query(connection, String.format("select * from %s", empNamePrivView2.toString()), new SQLClosures.SQLAction<ResultSet>() {
                    @Override
                    public void execute(ResultSet resultSet) throws Exception {
                        Assert.assertEquals(4, resultSetSize(resultSet));
                    }
                });
            }

            connection.rollback();
        }finally{
            connection.close();
        }
    }

    @Test(expected=SQLException.class)
     public void testRenameTableWithView() throws Exception {
        Connection connection = methodWatcher.createConnection();
        try{
            connection.setAutoCommit(false);
            SQLClosures.query(connection, String.format("select * from %s", empNamePrivView.toString()), new SQLClosures.SQLAction<ResultSet>() {
                @Override
                public void execute(ResultSet resultSet) throws Exception {
                    Assert.assertEquals(5, resultSetSize(resultSet));
                }
            });

            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format("rename table %s.%s to %s", tableSchema.schemaName, EMP_PRIV_TABLE2, "real_private"));
                }
            });

            connection.rollback();
        }finally{
            connection.close();
        }
    }
}
