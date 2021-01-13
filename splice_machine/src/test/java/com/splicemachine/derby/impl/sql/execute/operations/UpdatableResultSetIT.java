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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;
import java.util.Properties;

import static com.splicemachine.pipeline.ErrorState.CANNOT_ROLLBACK_CONFLICTING_TXN;
import static com.splicemachine.pipeline.ErrorState.WRITE_WRITE_CONFLICT;

@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class UpdatableResultSetIT {

    private static final String SCHEMA = UpdatableResultSetIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher  spliceClassWatcher = new SpliceWatcher(SCHEMA);

    protected static final String A_TABLE     = "A";
    protected static final String B_TABLE     = "B";
    protected static final String C_TABLE     = "C";
    protected static final String D_TABLE     = "D";
    protected static final String D_TABLE_IDX = "D_IDX";
    protected static final String E_TABLE     = "E";
    protected static final String F_TABLE     = "F";
    protected static final String F_TABLE_IDX = "F_IDX";
    protected static final String G_TABLE     = "G";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private TestConnection conn;
    private String         connectionString;
    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO     triggerDAO;

    private static final String DEFAULT = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin";
    private static final String CONTROL = DEFAULT + ";useSpark=false";
    private static final String SPARK = DEFAULT + ";useSpark=true";
    private String tableName;

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

    private void prepareTableData(String tableName) throws SQLException {
        conn.execute(String.format("INSERT INTO %s VALUES (2, 'helium')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (10, 'neon')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (18, 'argon')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (38, 'krypton')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (54, 'xenon')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (86, 'radon')", tableName));
        conn.execute(String.format("INSERT INTO %s VALUES (118, 'oganesson')", tableName));
    }

    @Before
    public void setUp() throws Exception{
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        triggerDAO = new TriggerDAO(conn);
        conn.setAutoCommit(true);
        conn.setSchema(SCHEMA);
        conn.execute("DELETE FROM A");
        conn.execute("DELETE FROM B");
        conn.execute("DELETE FROM C");
        conn.execute("DELETE FROM D");
        triggerDAO.dropAllTriggers(SCHEMA, E_TABLE);
        conn.execute("DELETE FROM E");
        conn.execute("DELETE FROM F");
        conn.execute("DELETE FROM G");
        prepareTableData(A_TABLE);
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
        new TableCreator(conn).withCreate(String.format("create table %s (c1 int, c2 varchar(20))", E_TABLE)).create();
        new TableCreator(conn).withCreate(String.format("create table %s (c1 char(36) not null, c2 char(8) not null, c3 char(36) not null, c4 timestamp not null, " +
                                                                         "c5 decimal(10,0) not null, c6 char(1) not null, c7 varchar(10) for bit data not null default X'')", F_TABLE))
                .withConstraints("primary key(c1)")
                .withIndex(String.format("create index %s on %s(c3)", F_TABLE_IDX, F_TABLE)).create();
        new TableCreator(conn).withCreate(String.format("create table %s (c1 int, c2 varchar(20))", G_TABLE)).create();
    }

    private void tableShouldBe(String tableName, Object[][] expected) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM " + tableName + " ORDER BY C1 ASC")) {
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
        tableShouldBe("A", new Object[][]{
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
        tableShouldBe("A", new Object[][]{
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
        tableShouldBe("A", new Object[][]{});
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
        tableShouldBe("A", new Object[][]{
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
        tableShouldBe("A", new Object[][]{
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
        tableShouldBe("A", new Object[][]{
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
                    if(connectionString.contains("useSpark=false")) { // DB-11070
                        resultSet.updateBinaryStream(16, new ByteArrayInputStream(new byte[]{0x1}), 1);
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
                if(connectionString.contains("useSpark=false")) { // DB-11070
                    Assert.assertEquals(1, resultSet.getBinaryStream(16).read());
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

    @Test
    public void testInteractionWithTriggers() throws Exception {
        conn.execute(tb.on(E_TABLE).named("trig01").after().update().row().referencing("old as o").then(String.format("INSERT INTO %s VALUES(o.c1 + 1019, o.c2)", E_TABLE)).build());
        conn.execute(tb.on(E_TABLE).named("trig02").after().delete().row().referencing("old as o").then(String.format("INSERT INTO %s VALUES(o.c1 + 1693, o.c2)", E_TABLE)).build());
        prepareTableData(E_TABLE);
        conn.execute(tb.on(E_TABLE).named("trig03").before().insert().row().referencing("new as n").then("SET n.c1 = n.c1 + 2957, n.c2 = n.c2").build());

        // test update-interaction between updatable cursors and triggers.
        try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT * FROM %s", E_TABLE), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }
        tableShouldBe(E_TABLE, new Object[][]{
                {2, "HELIUM"},
                {10, "NEON"},
                {18, "ARGON"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"},
                {1019 + 2957 + 2, "helium"},
                {1019 + 2957 + 10, "neon"},
                {1019 + 2957 + 18, "argon"},
                {1019 + 2957 + 38, "krypton"},
                {1019 + 2957 + 54, "xenon"},
                {1019 + 2957 + 86, "radon"},
                {1019 + 2957 + 118, "oganesson"},
        });

        // test delete-interaction between updatable cursors and triggers.
        try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT * FROM %s where c1 >= 1000", E_TABLE), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.deleteRow();
                }
            }
        }
        tableShouldBe(E_TABLE, new Object[][]{
                {2, "HELIUM"},
                {10, "NEON"},
                {18, "ARGON"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"},
                { /*old value*/ (1019 + 2957 + 2)  + /* delete trigger */ 1693 + /* insert trigger */ 2957, "helium"},
                { /*old value*/ (1019 + 2957 + 10) + /* delete trigger */ 1693 + /* insert trigger */ 2957, "neon"},
                { /*old value*/ (1019 + 2957 + 18) + /* delete trigger */ 1693 + /* insert trigger */ 2957, "argon"},
                { /*old value*/ (1019 + 2957 + 38) + /* delete trigger */ 1693 + /* insert trigger */ 2957, "krypton"},
                { /*old value*/ (1019 + 2957 + 54) + /* delete trigger */ 1693 + /* insert trigger */ 2957, "xenon"},
                { /*old value*/ (1019 + 2957 + 86) + /* delete trigger */ 1693 + /* insert trigger */ 2957, "radon"},
                { /*old value*/ (1019 + 2957 + 118)+ /* delete trigger */ 1693 + /* insert trigger */ 2957, "oganesson"},
        });

        // test insert-interaction between updatable cursors and triggers.
        try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT * FROM %s where c1 >= 2000", E_TABLE), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.moveToInsertRow();
                    resultSet.updateInt(1, 5881);
                    resultSet.updateString(2, "new row");
                    resultSet.insertRow();
                }
            }
        }
        tableShouldBe(E_TABLE, new Object[][]{
                {2, "HELIUM"},
                {10, "NEON"},
                {18, "ARGON"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"},
                { (1019 + 2957 + 2)  + 1693 + 2957, "helium"},
                { (1019 + 2957 + 10) + 1693 + 2957, "neon"},
                { (1019 + 2957 + 18) + 1693 + 2957, "argon"},
                { (1019 + 2957 + 38) + 1693 + 2957, "krypton"},
                { (1019 + 2957 + 54) + 1693 + 2957, "xenon"},
                { (1019 + 2957 + 86) + 1693 + 2957, "radon"},
                { (1019 + 2957 + 118)+ 1693 + 2957, "oganesson"},
                // inserted by the updatable cursor + modified by the insert triggered (+ 2957)
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"},
                { /* updatable cursor addition */ 5881 + /* insert trigger */ 2957, "new row"}
        });
    }

    private void testConcurrentUpdates(String tableName) throws Exception {
        try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT * FROM %s WHERE c1 = 18", tableName), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next(); // {18, "argon"}
                // update {18, "argon"} with a concurrent connection
                try(Connection conn2 = new TestConnection(DriverManager.getConnection(connectionString, new Properties()))) {
                    conn2.setSchema(SCHEMA);
                    try(Statement stmt2 = conn2.createStatement()) {
                        stmt2.executeUpdate(String.format("UPDATE %s SET c1 = 1, c2 = 'hydrogen' WHERE c1 = 18", tableName));
                    }
                }
                resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                try {
                    resultSet.updateRow();
                    Assert.fail("should've failed with an exception containing: Write Conflict detected between transactions");
                } catch(Exception e) {
                    Assert.assertTrue(e instanceof SQLException);
                    SQLException sqlException = (SQLException)e;
                    Assert.assertEquals(WRITE_WRITE_CONFLICT.getSqlState(), sqlException.getSQLState());
                }
            }
        }
        tableShouldBe(tableName, new Object[][]{
                {1, "hydrogen"},
                {2, "helium"},
                {10, "neon"},
                {38, "krypton"},
                {54, "xenon"},
                {86, "radon"},
                {118, "oganesson"}
        });
    }

    @Test
    public void testWithConcurrentUpdatesOnTableWithoutPrimaryKeys() throws Exception {
        testConcurrentUpdates(A_TABLE);
    }

    @Test
    public void testWithConcurrentUpdatesOnPrimaryKeyColumns() throws Exception {
        prepareTableData(D_TABLE);
        testConcurrentUpdates(D_TABLE);
    }

    private void prepareTableFData() throws SQLException {
        conn.execute(String.format("INSERT INTO %s VALUES('value0', 'value0', 'value0', '2020-12-21 20:54:09.123456', 3.14, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value1', 'value0', 'value1', '2020-12-20 20:54:09.123456', 3.15, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value2', 'value0', 'value2', '2020-12-19 20:54:09.123456', 3.16, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value3', 'value0', 'value3', '2020-12-18 20:54:09.123456', 3.17, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value4', 'value0', 'value4', '2020-12-17 20:54:09.123456', 3.18, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value5', 'value5', 'value5', '2020-12-16 20:54:09.123456', 3.18, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value6', 'value6', 'value6', '2020-12-15 20:54:09.123456', 3.20, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value7', 'value7', 'value7', '2020-12-14 20:54:09.123456', 3.21, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value8', 'value8', 'value8', '2020-12-13 20:54:09.123456', 3.22, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
        conn.execute(String.format("INSERT INTO %s VALUES('value9', 'value9', 'value9', '2020-12-12 20:54:09.123456', 3.23, 'A', CAST('abc' AS VARCHAR(32000) FOR BIT DATA))", F_TABLE));
    }

    private void tableFShouldBe(Object[][] expected) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT c3, c4, c7 FROM %s ORDER BY c3 ASC", F_TABLE))) {
            int row = 0;
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Assert.assertEquals(expected[row][0], resultSet.getString(1));
                    Assert.assertEquals(expected[row][1], resultSet.getTimestamp(2));
                    Assert.assertEquals(expected[row][2], resultSet.getString(3));
                    row++;
                }
            }
        }

    }

    private void testUpdatingPartialProjectionInternal(boolean useIndex) throws Exception {
        prepareTableFData();
        final String partialProjectionSql = String.format("SELECT c3, c4, c7 FROM %s --splice-properties index=%s\n WHERE c6 = 'A' AND c2 = ? AND c3 BETWEEN ? and ? OPTIMIZE FOR 1 ROWS WITH UR",
                                                          F_TABLE, (useIndex ? F_TABLE_IDX : "null"));

        tableFShouldBe(new Object[][]{
                {"value0                              ", Timestamp.valueOf("2020-12-21 20:54:09.123456"), "616263"},
                {"value1                              ", Timestamp.valueOf("2020-12-20 20:54:09.123456"), "616263"},
                {"value2                              ", Timestamp.valueOf("2020-12-19 20:54:09.123456"), "616263"},
                {"value3                              ", Timestamp.valueOf("2020-12-18 20:54:09.123456"), "616263"},
                {"value4                              ", Timestamp.valueOf("2020-12-17 20:54:09.123456"), "616263"},
                {"value5                              ", Timestamp.valueOf("2020-12-16 20:54:09.123456"), "616263"},
                {"value6                              ", Timestamp.valueOf("2020-12-15 20:54:09.123456"), "616263"},
                {"value7                              ", Timestamp.valueOf("2020-12-14 20:54:09.123456"), "616263"},
                {"value8                              ", Timestamp.valueOf("2020-12-13 20:54:09.123456"), "616263"},
                {"value9                              ", Timestamp.valueOf("2020-12-12 20:54:09.123456"), "616263"}
        });

        try (PreparedStatement statement = conn.prepareStatement(partialProjectionSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setString(1, "value0");
            statement.setString(2, "value0");
            statement.setString(3, "value9");

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.updateString(1, resultSet.getString(1).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }

        tableFShouldBe(new Object[][]{
                {"VALUE0                              ", Timestamp.valueOf("2020-12-21 20:54:09.123456"), "616263"},
                {"VALUE1                              ", Timestamp.valueOf("2020-12-20 20:54:09.123456"), "616263"},
                {"VALUE2                              ", Timestamp.valueOf("2020-12-19 20:54:09.123456"), "616263"},
                {"VALUE3                              ", Timestamp.valueOf("2020-12-18 20:54:09.123456"), "616263"},
                {"VALUE4                              ", Timestamp.valueOf("2020-12-17 20:54:09.123456"), "616263"},
                {"value5                              ", Timestamp.valueOf("2020-12-16 20:54:09.123456"), "616263"},
                {"value6                              ", Timestamp.valueOf("2020-12-15 20:54:09.123456"), "616263"},
                {"value7                              ", Timestamp.valueOf("2020-12-14 20:54:09.123456"), "616263"},
                {"value8                              ", Timestamp.valueOf("2020-12-13 20:54:09.123456"), "616263"},
                {"value9                              ", Timestamp.valueOf("2020-12-12 20:54:09.123456"), "616263"}
        });
    }

    @Test
    public void testUpdatingPartialProjection() throws Exception {
        testUpdatingPartialProjectionInternal(false);
    }

    @Test
    public void testUpdatingPartialProjectionWithIndexScan() throws Exception {
        testUpdatingPartialProjectionInternal(true);
    }

    @Test
    public void testUpdatingSubqueryNotSupported() throws Exception {
        prepareTableData(G_TABLE);
        final String subquerySql = "SELECT c1, c2 FROM %s WHERE c2 in (select c2 FROM %s --splice-properties index=%s\nWHERE c1 >= ?)";
        try (PreparedStatement statement = conn.prepareStatement(String.format(subquerySql, G_TABLE, A_TABLE, "null"), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 35);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    try {
                        resultSet.updateString(2, resultSet.getString(1).toUpperCase());
                    } catch(Exception e) {
                        Assert.assertTrue(e instanceof SQLException);
                        SQLException sqlException = (SQLException)e;
                        Assert.assertTrue(sqlException.getMessage().contains("'updateString' not allowed because the ResultSet is not an updatable ResultSet."));
                    }
                }
            }
        }
    }
}

