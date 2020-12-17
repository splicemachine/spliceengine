/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.dbTesting.functionTests.util.streams.ByteAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetStream;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;
import java.util.Properties;

@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class UpdatableResultSetIT {

    private static final String SCHEMA = UpdatableResultSetIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    protected static final String A_TABLE     = "A";
    protected static final String B_TABLE     = "B";
    protected static final String C_TABLE     = "C";
    protected static final String D_TABLE     = "D";
    protected static final String D_TABLE_IDX = "D_IDX";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private TestConnection conn;
    private String         connectionString;

    private static final String DEFAULT = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin";
    private static final String CONTROL = DEFAULT + ";useSpark=false";
    private static final String SPARK = DEFAULT + ";useSpark=true";

    @Parameterized.Parameters(name = "with mode {0}")
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{SPARK});
        params.add(new Object[]{CONTROL});
        params.add(new Object[]{DEFAULT});
        return params;
    }

    public UpdatableResultSetIT(String connectionString) throws Exception {
        this.connectionString = connectionString;
    }

    @Before
    public void setUp() throws Exception{
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setAutoCommit(true);
        conn.setSchema(SCHEMA);
        conn.execute("DELETE FROM A");
        conn.execute("DELETE FROM B");
        conn.execute("DELETE FROM C");
        conn.execute("DELETE FROM D");
        conn.execute("INSERT INTO A VALUES (2, 'helium')");
        conn.execute("INSERT INTO A VALUES (10, 'neon')");
        conn.execute("INSERT INTO A VALUES (18, 'argon')");
        conn.execute("INSERT INTO A VALUES (38, 'krypton')");
        conn.execute("INSERT INTO A VALUES (54, 'xenon')");
        conn.execute("INSERT INTO A VALUES (86, 'radon')");
        conn.execute("INSERT INTO A VALUES (118, 'oganesson')");
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @BeforeClass
    public static void createdSharedTables() throws Exception {
       TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn).withCreate(String.format("create table %s (c1 int, c2 varchar(10))", A_TABLE)).create();
        new TableCreator(conn).withCreate(String.format("create table %s (a smallint," +
                                                                "b integer," +
                                                                "c bigint," +
                                                                "d decimal(16, 10)," +
                                                                "e numeric(16, 10)," +
                                                                "f real," +
                                                                "g double," +
                                                                "h float," +
                                                                "i decfloat," +
                                                                "j char(3)," +
                                                                "k varchar(3)," +
                                                                "l timestamp," +
                                                                "m time," +
                                                                "n date," +
                                                                "o clob(3)," +
                                                                "p blob(3))", B_TABLE)).create();

        new TableCreator(conn).withCreate(String.format("create table %s (c1 int, c2 varchar(20))", C_TABLE)).create();
        new TableCreator(conn).withCreate(String.format("create table %s (c1 int primary key, c2 varchar(20))", D_TABLE)).withIndex(String.format("create index %s on %s(c1)", D_TABLE_IDX, D_TABLE)).create();
    }

    private void tableAshouldBe(Object[][] expected) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A ORDER BY C1 ASC")) {
            int row = 0;
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Assert.assertEquals(expected[row][0], resultSet.getInt(1));
                    Assert.assertEquals(expected[row][1], resultSet.getString(2));
                    row++;
                }
            }
        }
    }

    @Test
    public void testUpdate() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "HELIUM"},
                {10, "NEON"},
                {18, "ARGON"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"}});
    }

    @Test
    public void testUpdateWithFilter() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A WHERE C1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 20);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {10, "neon"},
                {18, "argon"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"}});
    }

    @Test
    public void testDelete() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.deleteRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{});
    }

    @Test
    public void testDeleteWithFilter() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A WHERE C1 BETWEEN ? AND ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 10);
            statement.setInt(2, 40);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.deleteRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {54, "xenon"},
                {86, "radon"},
                {118, "oganesson"}});
    }

    @Test
    public void testInsert() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int c1 = resultSet.getInt(1);
                    String c2 = resultSet.getString(2);
                    resultSet.moveToInsertRow();
                    resultSet.updateInt(1, c1);
                    resultSet.updateString(2, c2);
                    resultSet.insertRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {2, "helium"},
                {10, "neon"},
                {10, "neon"},
                {18, "argon"},
                {18, "argon"},
                {38, "krypton"},
                {38, "krypton"},
                {54, "xenon"},
                {54, "xenon"},
                {86, "radon"},
                {86, "radon"},
                {118, "oganesson"},
                {118, "oganesson"}
        });
    }

    @Test
    public void testInsertWithFilter() throws Exception {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM A WHERE c1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 20);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int c1 = resultSet.getInt(1);
                    String c2 = resultSet.getString(2);
                    resultSet.moveToInsertRow();
                    resultSet.updateInt(1, c1);
                    resultSet.updateString(2, c2);
                    resultSet.insertRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {10, "neon"},
                {18, "argon"},
                {38, "krypton"},
                {38, "krypton"},
                {54, "xenon"},
                {54, "xenon"},
                {86, "radon"},
                {86, "radon"},
                {118, "oganesson"},
                {118, "oganesson"}
        });
    }



    @Test
    public void testDataTypes() throws Exception {
        conn.execute("INSERT INTO B VALUES(" +
                                            "1," +
                                            "2," +
                                            "3," +
                                            "4.5," +
                                            "6," +
                                            "7.8," +
                                            "9.10," +
                                            "11.12," +
                                            "13.14," +
                                            "'a'," +
                                            "'b'," +
                                            "timestamp('2020-12-14 15:21:30.123456')," +
                                            "time('15:21:30')," +
                                            "date('2020-12-14')," +
                                            "'c'," +
                                            "CAST (X'10' AS BLOB))");
        ByteAlphabet a1 = ByteAlphabet.singleByte((byte) 8);
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM B", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.updateInt(1, 10);
                    resultSet.updateInt(2, 20);
                    resultSet.updateLong(3, 30);
                    resultSet.updateBigDecimal(4, new BigDecimal("40.5000000000"));
                    resultSet.updateBigDecimal(5, new BigDecimal("60.1000000000"));
                    resultSet.updateFloat(6, 75.76f);
                    resultSet.updateFloat(7, 80.9f);
                    resultSet.updateDouble(8, 90.11d);
                    resultSet.updateBigDecimal(9, new BigDecimal("100.11"));
                    resultSet.updateString(10, "ZZZ");
                    resultSet.updateString(11, "Y");
                    resultSet.updateTimestamp(12, Timestamp.valueOf("2021-01-14 15:21:30.123456"));
                    resultSet.updateTime(13, Time.valueOf("16:21:30"));
                    resultSet.updateDate(14, Date.valueOf("2021-01-14"));
                    if(connectionString.contains("userSpark=false")) { // DB-11070
                        resultSet.updateBinaryStream(16, new LoopingAlphabetStream(1, a1), 1);
                    }
                    resultSet.updateRow();
                }
            }
        }

        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM B")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(10, resultSet.getInt(1));
                Assert.assertEquals(20, resultSet.getInt(2));
                Assert.assertEquals(30, resultSet.getLong(3));
                Assert.assertEquals(new BigDecimal("40.5000000000"), resultSet.getBigDecimal(4));
                Assert.assertEquals(new BigDecimal("60.1000000000"), resultSet.getBigDecimal(5));
                Assert.assertEquals(75.76f, resultSet.getFloat(6), 0.01f);
                Assert.assertEquals(80.9f, resultSet.getFloat(7), 0.01f);
                Assert.assertEquals(90.11d, resultSet.getDouble(8), 0.01f);
                Assert.assertEquals(new BigDecimal("100.11"), resultSet.getBigDecimal(9));
                Assert.assertEquals("ZZZ", resultSet.getString(10));
                Assert.assertEquals("Y", resultSet.getString(11));
                Assert.assertEquals(Timestamp.valueOf("2021-01-14 15:21:30.123456"), resultSet.getTimestamp(12));
                Assert.assertEquals(Time.valueOf("16:21:30"), resultSet.getTime(13));
                Assert.assertEquals(Date.valueOf("2021-01-14"), resultSet.getDate(14));
                if(connectionString.contains("userSpark=false")) { // DB-11070
                    Assert.assertEquals(new LoopingAlphabetStream(1, a1).read(), resultSet.getBinaryStream(16).read());
                }
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testUpdateOnDistributedCursor() throws Exception {
        int rowCount = 7000, rangeA = 400, rangeB = 405;
        if(connectionString.contains("useSpark=true")) {
            rowCount = 100; rangeA = 50; rangeB = 55;
        }
        try(PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO C VALUES (?, ?)")) {
            for(int i = 0; i <= rowCount; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "value " + i);
                preparedStatement.addBatch();
                if(i % 1000 == 0) {
                    preparedStatement.executeBatch();
                }
            }

        }

        final String expectedValue = "NEW VALUE!";
        String sql = "SELECT * FROM C WHERE c1 between ? and ?";
        try(PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setInt(1, rangeA);
            ps.setInt(2, rangeB);
            ResultSet result = ps.executeQuery();
            while(result.next()) {
                result.updateString(2, expectedValue);
                result.updateRow();
            }
        }

        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, rangeA);
            ps.setInt(2, rangeB);
            ResultSet result = ps.executeQuery();
            while(result.next()) {
                Assert.assertEquals(expectedValue, result.getString(2));
            }
        }
    }

    @Test
    public void testUpdatesWithIndexScan() throws Exception {
        final int rowCount = 100;
        try(PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO D VALUES (?, ?)")) {
            for(int i = 0; i < rowCount; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "value " + i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }

        String rowId = "";
        final String expectedValue = "NEW VALUE!";

        // get the original rowId to make sure we didn't change it after the update.
        String sql = String.format("SELECT rowid,c2 FROM %s --splice-properties index=%s\nWHERE c1 = ?", D_TABLE, D_TABLE_IDX);
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 50);
            ResultSet result = ps.executeQuery();
            Assert.assertTrue(result.next());
            rowId = result.getString(1);
            Assert.assertFalse(result.next());
        }

        try(PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM %s --splice-properties index=%s\nWHERE c1 = ?", D_TABLE, D_TABLE_IDX), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setInt(1, 50);
            ResultSet result = ps.executeQuery();
            while(result.next()) {
                result.updateString(2, expectedValue);
                result.updateRow();
            }
        }

        // verify
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 50);
            ResultSet result = ps.executeQuery();
            Assert.assertTrue(result.next());
            Assert.assertEquals(rowId, result.getString(1));
            Assert.assertEquals(expectedValue, result.getString(2));
            Assert.assertFalse(result.next());
        }
    }
}

