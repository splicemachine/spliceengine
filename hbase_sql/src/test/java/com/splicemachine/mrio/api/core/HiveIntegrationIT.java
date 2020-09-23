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

package com.splicemachine.mrio.api.core;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_tools.TableCreator;
@Ignore
public class HiveIntegrationIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(HiveIntegrationIT.class);
    public static final String CLASS_NAME = HiveIntegrationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static String driverName = HiveDriver.class.getCanonicalName();
    static {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private TriggerBuilder tb = new TriggerBuilder();

    public static void createData(Connection conn) throws Exception {
        new TableCreator(conn)
            .withCreate("create table A(col1 int, col2 int, col3 int, primary key (col3, col1))")
            .create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into A values (?,?,?)");
        for (int i = 0; i< 100; i++) {
            insert.setInt(1, i);
            insert.setInt(2, i + 1);
            insert.setInt(3, i + 2);
            insert.executeUpdate();
        }

        new TableCreator(conn)
            .withCreate("create table B(col1 char(20), col2 varchar(56), primary key (col1))")
            .create();

        insert = spliceClassWatcher.prepareStatement("insert into B values (?,?)");
        for (int i = 0; i< 100; i++) {
            insert.setString(1, "Char " + i);
            insert.setString(2, "Varchar " + i);
            insert.executeUpdate();
        }

        new TableCreator(conn)
            .withCreate("create table C("
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
            )
            .create();

        insert = spliceClassWatcher.prepareStatement("insert into C values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for (int i = 0; i< 100; i++) {
            insert.setInt(1, i);
            insert.setInt(2, i);
            insert.setInt(3, i);
            insert.setInt(4, i);
            insert.setFloat(5, (float) (i * 1.0));
            insert.setDouble(6, i * 1.0);
            insert.setBigDecimal(7, new BigDecimal(i * 1.0));
            insert.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
            insert.setDate(9, new Date(System.currentTimeMillis()));
            insert.setString(10, "varchar " + i);
            insert.setString(11, "char " + i);
            insert.setBoolean(12, true);
            insert.setString(13, "Binary " + i);
            insert.executeUpdate();
        }

        new TableCreator(conn)
            .withCreate("create table D (id int, name varchar(10), gender char(1))")
            .withInsert("insert into D values(?,?,?)")
            .withRows(rows(
                row(1, null, "M")))
            .create();

        new TableCreator(conn)
            .withCreate("create table E ("
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
                + "boolean_col boolean)")
            .create();

        insert = spliceClassWatcher.prepareStatement("insert into E ("
            + "decimal_col, "
            + "timestamp_col, "
            + "date_col, "
            + "varchar_col, "
            + "char_col)"
            + "values (?,?,?,?,?)");

        for (int i = 0; i< 100; i++) {
            insert.setBigDecimal(1, null);
            insert.setTimestamp(2, null);
            insert.setDate(3, null);
            insert.setString(4, null);
            insert.setString(5, null);
            insert.executeUpdate();
        }

        new TableCreator(conn)
            .withCreate("create table F ("
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
                + "boolean_col boolean)")
            .create();

        new TableCreator(conn)
            .withCreate("create table G (col1 int, col2 int, col3 int)")
            .create();

        new TableCreator(conn)
            .withCreate("create table H (col1 int, col2 int, col3 int, primary key (col3, col1))")
            .create();

        new TableCreator(conn)
            .withCreate("create table I (message varchar(1024))")
            .create();

        new TableCreator(conn)
            .withCreate("create table J (col1 int, col2 int, col3 int constraint col3_ck1 check (col3 > 0))")
            .create();

        new TableCreator(conn)
            .withCreate("create table K (a int primary key, b int, CONSTRAINT fk FOREIGN KEY (B) REFERENCES K(a))")
            .withInsert("insert into K values(?,?)")
            .withRows(rows(row(1, null), row(2, null), row(3, 1), row(4, 1), row(5, 1), row(6, 1))).create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        FileSystem fs = FileSystem.get(URI.create(getHiveWarehouseDirectory()),
                                       (Configuration) SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate());
        fs.delete(new Path(getBaseDirectory()+"/user"), true);
        fs.delete(new Path(getBaseDirectory() + "/../target"), true);
    }

    @Test
    public void testCompositePK() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive2://");
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
            Assert.assertTrue(v2 == v1 + 1);
            Assert.assertTrue(v3 == v2 + 1);

        }
        Assert.assertEquals("incorrect number of rows returned", 100, i);
    }

    @Test
    public void testVarchar() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive2://");
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
        Connection con = DriverManager.getConnection("jdbc:hive2://");
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
            Assert.assertTrue("Incorrect gender value returned", gender.compareToIgnoreCase("M") == 0);
        }
        Assert.assertEquals("incorrect number of rows returned", 1,i);
    }

    @Test
    public void testDataTypes() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive2://");
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
        Connection con = DriverManager.getConnection("jdbc:hive2://");
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
            Assert.assertTrue("col1 did not return", rs.getByte(1) == 0);
            Assert.assertTrue("col2 did not return", rs.getShort(2) == 0);
            Assert.assertTrue("col3 did not return", rs.getInt(3) == 0);
            Assert.assertTrue("col4 did not return", rs.getLong(4) == 0);
            Assert.assertTrue("col5 did not return", rs.getFloat(5) == 0);
            Assert.assertTrue("col6 did not return", rs.getDouble(6) == 0);
            //TODO - jyuan: Should this return a null?
            Assert.assertTrue("col7 did not return", rs.getBigDecimal(7).toString().compareTo("0") == 0);
            Assert.assertNull("col8 did not return", rs.getTimestamp(8));
            Assert.assertNull("col9 did not return", rs.getDate(9));
            Assert.assertNull("col10 did not return", rs.getString(10));
            Assert.assertNull("col11 did not return", rs.getString(11));
            Assert.assertTrue("col12 did not return", rs.getBoolean(12) == false);
        }
        Assert.assertEquals("incorrect number of rows returned", 100, i);
    }

    @Test
    public void testInsert() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive2://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE F ("
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
            "\"splice.tableName\" = \"HIVEINTEGRATIONIT.F\""+
            ")";

        stmt.execute(createExternalExisting);
        PreparedStatement ps = con.prepareStatement("insert into table F values(?,?,?,?,?,?,?,?,?,?,?,?)");
        int nrows = 10;
        for (int i = 0; i < nrows; ++i) {
            ps.setByte(1, (byte)i);
            ps.setShort(2, (short) i);
            ps.setInt(3, i);
            ps.setLong(4, i);
            ps.setFloat(5, (float)1.0*i);
            ps.setDouble(6, 1.0*i);
            ps.setDouble(7, 1.0 * i);
            ps.setString(8, (new Timestamp(System.currentTimeMillis())).toString());
            ps.setString(9, (new Date(System.currentTimeMillis())).toString());
            ps.setString(10, "varchar " + i);
            ps.setString(11, "char " + i);
            ps.setBoolean(12, true);
            ps.execute();
        }

        ResultSet rs = stmt.executeQuery("select * from F");
        int i = 0;
        while (rs.next()) {
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
            i++;
        }
        Assert.assertEquals("incorrect number of rows returned", nrows, i);
    }

    @Test
    @Ignore("Fails due to general trigger issues not specific to hive integration")
    public void testInsertFireTrigger() throws Exception {

        createTrigger(tb.on("HIVEINTEGRATIONIT.G").named("trig").after().insert().statement().then("INSERT INTO HIVEINTEGRATIONIT.I VALUES('inserted a row')"));

        Connection con = DriverManager.getConnection("jdbc:hive2://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE G " +
            "(COL1 INT, COL2 INT, COL3 INT) " +
            "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
            "TBLPROPERTIES (" +
            "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
            "\"splice.tableName\" = \"HIVEINTEGRATIONIT.G\""+
            ")";
        stmt.execute(createExternalExisting);
        PreparedStatement ps = con.prepareStatement("insert into table G values (?,?,?)");

        int nrows = 10;
        for (int i = 0; i < nrows; ++i) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3, i);
            ps.execute();
        }

        ResultSet rs = stmt.executeQuery("select * from G");
        int i = 0;
        while (rs.next()) {
            int col1 = rs.getInt(1);
            int col2 = rs.getInt(2);
            int col3 = rs.getInt(3);

            Assert.assertTrue(col2 == col1);
            Assert.assertTrue(col3 == col2);
            i++;
        }

        Assert.assertTrue(i == nrows);

        rs = methodWatcher.executeQuery("select count(*) from hiveintegrationit.i");
        Assert.assertTrue(rs.next());
        Assert.assertTrue(nrows == rs.getInt(1));
    }

    @Test
    public void testInsertTableWithPKColumns() throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:hive2://");
        Statement stmt = con.createStatement();
        String createExternalExisting = "CREATE EXTERNAL TABLE H " +
            "(COL1 INT, COL2 INT, COL3 INT) " +
            "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
            "TBLPROPERTIES (" +
            "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
            "\"splice.tableName\" = \"HIVEINTEGRATIONIT.H\""+
            ")";
        stmt.execute(createExternalExisting);
        PreparedStatement ps = con.prepareStatement("insert into table H values (?,?,?)");

        int nrows = 10;
        for (int i = 0; i < nrows; ++i) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3, i);
            ps.execute();
        }

        ResultSet rs = stmt.executeQuery("select * from H");
        int i = 0;
        while (rs.next()) {
            int col1 = rs.getInt(1);
            int col2 = rs.getInt(2);
            int col3 = rs.getInt(3);

            Assert.assertTrue(i==col1);
            Assert.assertTrue(i==col2);
            Assert.assertTrue(i==col3);
            i++;
        }

        Assert.assertTrue(i == nrows);
    }

    @Test
    public void testCheckConstraint() throws Exception {

        try {
            Connection con = DriverManager.getConnection("jdbc:hive2://");
            Statement stmt = con.createStatement();
            String createExternalExisting = "CREATE EXTERNAL TABLE J " +
                "(COL1 INT, COL2 INT, COL3 INT) " +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \"" + SpliceNetConnection.getDefaultLocalURL() + "\"," +
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.J\"" +
                ")";
            stmt.execute(createExternalExisting);
            PreparedStatement ps = con.prepareStatement("insert into table J values (?,?,?)");
            ps.setInt(1, 100);
            ps.setInt(2, 100);
            ps.setInt(3, -100);
            ps.execute();
            fail("Expected constraint violation");
        } catch (Exception e) {
        }
    }

    @Test
    public void testSelfReferencingForeignKey() throws Exception {

        try {
            Connection con = DriverManager.getConnection("jdbc:hive2://");
            Statement stmt = con.createStatement();
            String createExternalExisting = "CREATE EXTERNAL TABLE K " +
                "(a INT, b INT) " +
                "STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
                "TBLPROPERTIES (" +
                "\"splice.jdbc\" = \"" + SpliceNetConnection.getDefaultLocalURL() + "\"," +
                "\"splice.tableName\" = \"HIVEINTEGRATIONIT.K\"" +
                ")";
            stmt.execute(createExternalExisting);
            PreparedStatement ps = con.prepareStatement("insert into table J values (?,?)");
            ps.setInt(1, 10);
            ps.setInt(2, 100);
            ps.execute();
            fail("Expected constraint violation");
        } catch (Exception e) {
        }
    }

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }

}
