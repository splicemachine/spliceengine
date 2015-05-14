package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class HiveIntegrationIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(HiveIntegrationIT.class);
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    static {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(HiveIntegrationIT.class.getSimpleName());
    protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",HiveIntegrationIT.class.getSimpleName(),"(col1 int, col2 int, col3 int, primary key (col3, col1))");
    protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",HiveIntegrationIT.class.getSimpleName(),"(col1 char(20), col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherC = new SpliceTableWatcher("C",HiveIntegrationIT.class.getSimpleName(),"("
            + "tinyint_col smallint,"
            + "smallint_col smallInt, "
            + "int_col int, "
            + "bigint_col bigint, "
            + "float_col float, "
            + "double_col double, "
            + "decimal_col decimal, "
            + "timestamp_col timestamp, "
            + "date_col date, "
            + "varchar_col varchar(32), "
            + "char_col char(32), "
            + "boolean_col boolean, "
            + "binary_col varchar(30))"
    );


    protected static SpliceTableWatcher spliceTableWatcherD = new SpliceTableWatcher("D",HiveIntegrationIT.class.getSimpleName(),"(id int, name varchar(10), gender char(1))");
    protected static SpliceTableWatcher spliceTableWatcherE = new SpliceTableWatcher("E",HiveIntegrationIT.class.getSimpleName(),"("
            + "tinyint_col smallint,"
            + "smallint_col smallInt, "
            + "int_col int, "
            + "bigint_col bigint, "
            + "float_col float, "
            + "double_col double, "
            + "decimal_col decimal, "
            + "timestamp_col timestamp, "
            + "date_col date, "
            + "varchar_col varchar(32), "
            + "char_col char(32), "
            + "boolean_col boolean)"
    );
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcherA)
            .around(spliceTableWatcherB)
            .around(spliceTableWatcherC)
            .around(spliceTableWatcherE)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".A (col1,col2,col3) values (?,?,?)");
                        PreparedStatement psB = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".B (col1,col2) values (?,?)");
                        PreparedStatement psC = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".C ("
                                + "tinyint_col,"
                                + "smallint_col, "
                                + "int_col, "
                                + "bigint_col, "
                                + "float_col, "
                                + "double_col, "
                                + "decimal_col, "
                                + "timestamp_col, "
                                + "date_col, "
                                + "varchar_col, "
                                + "char_col, "
                                + "boolean_col, "
                                + "binary_col) "
                                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?)");

                        PreparedStatement psE = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".E ("
                                + "decimal_col, "
                                + "timestamp_col, "
                                + "date_col, "
                                + "varchar_col, "
                                + "char_col)"
                                + "values (?,?,?,?,?)");

                        for (int i = 0; i< 100; i++) {
                            psA.setInt(1,i);
                            psA.setInt(2,i+1);
                            psA.setInt(3,i+2);
                            psA.executeUpdate();

                            psB.setString(1,"Char " + i);
                            psB.setString(2, "Varchar " + i);
                            psB.executeUpdate();

                            psC.setInt(1, i);
                            psC.setInt(2, i);
                            psC.setInt(3, i);
                            psC.setInt(4, i);
                            psC.setFloat(5, (float) (i * 1.0));
                            psC.setDouble(6, i * 1.0);
                            psC.setBigDecimal(7, new BigDecimal(i*1.0));
                            psC.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                            psC.setDate(9, new Date(System.currentTimeMillis()));
                            psC.setString(10, "varchar " + i);
                            psC.setString(11, "char " + i);
                            psC.setBoolean(12, true);
                            psC.setString(13, "Binary " + i);
                            psC.executeUpdate();

                            psE.setBigDecimal(1, null);
                            psE.setTimestamp(2, null);
                            psE.setDate(3, null);
                            psE.setString(4, null);
                            psE.setString(5, null);
                            psE.executeUpdate();
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcherD)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement psD = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".D (id,name,gender) values (?,?,?)");
                        psD.setInt(1,1);
                        psD.setString(2, null);
                        psD.setString(3,"M");
                        psD.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCompositePK() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE A " +
                "(COL1 INT, COL2 INT, COL3 INT) " +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.A\""+
                ")";
        stmt.execute(createExternalExisting);

        ResultSet rs = stmt.executeQuery("select * from A");
        int i = 0;
        while (rs.next()) {
            i++;
            int v1 = rs.getInt(1);
            int v2 = rs.getInt(2);
            int v3 = rs.getInt(3);

            Assert.assertNotNull("col1 did not return", v1);
            Assert.assertNotNull("col1 did not return", v2);
            Assert.assertNotNull("col1 did not return", v3);
            Assert.assertTrue(v2==v1+1);
            Assert.assertTrue(v3==v2+1);

        }
        Assert.assertEquals("incorrect number of rows returned", 100,i);
    }

    @Test
    public void testVarchar() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE B " +
                "(col1 String, col2 VARCHAR(56)) " +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.B\""+
                ")";

        stmt.execute(createExternalExisting);
        ResultSet rs = stmt.executeQuery("select * from B");
        int i = 0;
        while (rs.next()) {
            i++;
            Assert.assertNotNull("col1 did not return", rs.getString(1));
            Assert.assertNotNull("col1 did not return", rs.getString(2));
        }
        Assert.assertEquals("incorrect number of rows returned", 100,i);
    }

    @Test
    public void testNullColumnValue() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE D " +
                "(id int, name VARCHAR(10), gender char(1)) " +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.D\""+
                ")";

        stmt.execute(createExternalExisting);
        ResultSet rs = stmt.executeQuery("select * from D");
        int i = 0;
        while (rs.next()) {
            i++;
            int id = rs.getInt(1);
            String name = rs.getString(2);
            String gender = rs.getString(3);
            Assert.assertNotNull("col1 did not return", id);
            Assert.assertNull("col2 did not return", name);
            Assert.assertTrue("Incorrect gender value returned", gender.compareToIgnoreCase("M")==0);
        }
        Assert.assertEquals("incorrect number of rows returned", 1,i);
    }

    @Test
    public void testDataTypes() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE C ("
                + "tinyint_col tinyint,"
                + "smallint_col smallInt, "
                + "int_col int, "
                + "bigint_col bigint, "
                + "float_col float, "
                + "double_col double, "
                + "decimal_col decimal, "
                + "timestamp_col timestamp, "
                + "date_col date, "
                + "varchar_col varchar(32), "
                + "char_col char(32), "
                + "boolean_col boolean, "
                + "binary_col binary)" +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.C\""+
                ")";

        stmt.execute(createExternalExisting);
        ResultSet rs = stmt.executeQuery("select * from C");
        int i = 0;
        while (rs.next()) {
            i++;
            Assert.assertNotNull("col1 did not return", rs.getByte(1));
            Assert.assertNotNull("col2 did not return", rs.getShort(2));
            Assert.assertNotNull("col3 did not return", rs.getInt(3));
            Assert.assertNotNull("col4 did not return", rs.getLong(4));
            Assert.assertNotNull("col5 did not return", rs.getFloat(5));
            Assert.assertNotNull("col6 did not return", rs.getDouble(6));
            Assert.assertNotNull("col7 did not return", rs.getBigDecimal(7));
            Assert.assertNotNull("col8 did not return", rs.getTimestamp(8));
            Assert.assertNotNull("col9 did not return", rs.getDate(9));
            Assert.assertNotNull("col10 did not return", rs.getString(10));
            Assert.assertNotNull("col11 did not return", rs.getString(11));
            Assert.assertNotNull("col12 did not return", rs.getBoolean(12));
            Assert.assertNotNull("col13 did not return", rs.getString(13));
        }
        Assert.assertEquals("incorrect number of rows returned", 100, i);
    }

    @Test
    public void testNullColumnValues() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE E ("
                + "tinyint_col tinyint,"
                + "smallint_col smallInt, "
                + "int_col int, "
                + "bigint_col bigint, "
                + "float_col float, "
                + "double_col double, "
                + "decimal_col decimal, "
                + "timestamp_col timestamp, "
                + "date_col date, "
                + "varchar_col varchar(32), "
                + "char_col char(32), "
                + "boolean_col boolean)" +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.E\""+
                ")";

        stmt.execute(createExternalExisting);
        ResultSet rs = stmt.executeQuery("select * from E");
        int i = 0;
        while (rs.next()) {
            i++;
            Assert.assertTrue("col1 did not return", rs.getByte(1)==0);
            Assert.assertTrue("col2 did not return", rs.getShort(2)==0);
            Assert.assertTrue("col3 did not return", rs.getInt(3)==0);
            Assert.assertTrue("col4 did not return", rs.getLong(4)==0);
            Assert.assertTrue("col5 did not return", rs.getFloat(5)==0);
            Assert.assertTrue("col6 did not return", rs.getDouble(6)==0);
            //TODO - jyuan: Should this return a null?
            Assert.assertTrue("col7 did not return", rs.getBigDecimal(7).toString().compareTo("0")==0);
            Assert.assertNull("col8 did not return", rs.getTimestamp(8));
            Assert.assertNull("col9 did not return", rs.getDate(9));
            Assert.assertNull("col10 did not return", rs.getString(10));
            Assert.assertNull("col11 did not return", rs.getString(11));
            Assert.assertTrue("col12 did not return", rs.getBoolean(12)==false);
        }
        Assert.assertEquals("incorrect number of rows returned", 100, i);
    }
}
