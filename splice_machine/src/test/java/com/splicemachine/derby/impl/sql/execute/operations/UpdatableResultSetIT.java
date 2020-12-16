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
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.*;

public class UpdatableResultSetIT {

    private static final   String              CLASS_NAME         = UpdatableResultSetIT.class.getSimpleName().toUpperCase();
    private static final   SpliceWatcher       spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final   SpliceSchemaWatcher schemaWatcher      = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final SpliceTableWatcher  A_TABLE            = new SpliceTableWatcher("A", schemaWatcher.schemaName,
                                                                                           "(c1 int, c2 varchar(10))");
    protected static final SpliceTableWatcher  B_TABLE            = new SpliceTableWatcher("B", schemaWatcher.schemaName,
                                                                                           "(a smallint," +
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
                                                                                                   "p blob(3))");
    protected static final SpliceTableWatcher  C_TABLE            = new SpliceTableWatcher("C", schemaWatcher.schemaName,"(c1 int, c2 varchar(20))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(B_TABLE)
            .around(C_TABLE);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Before
    public void prepareTableA() throws Exception {
        methodWatcher.executeUpdate("DELETE FROM A");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (2, 'helium')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (10, 'neon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (18, 'argon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (38, 'krypton')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (54, 'xenon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (86, 'radon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (118, 'oganesson')");
    }

    private void tableAshouldBe(Object[][] expected) throws SQLException {
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A ORDER BY C1 ASC")) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE C1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE C1 BETWEEN ? AND ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE c1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
        methodWatcher.executeUpdate("INSERT INTO B VALUES(" +
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
        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM B", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
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
                    resultSet.updateBinaryStream(16, new LoopingAlphabetStream(1, a1), 1);
                    resultSet.updateRow();
                }
            }
        }

        try (PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM B")) {
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
                Assert.assertEquals(new LoopingAlphabetStream(1, a1).read(), resultSet.getBinaryStream(16).read());
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testUpdateOnDistributedCursor() throws Exception {
        final int rowCount = 7000;
        try(PreparedStatement preparedStatement = methodWatcher.prepareStatement("INSERT INTO C VALUES (?, ?)")) {
            for(int i = 0; i < rowCount; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "value " + i);
                preparedStatement.addBatch();
            }
        }

        final String expectedValue = "NEW VALUE!";
        try(PreparedStatement ps = methodWatcher.prepareStatement("SELECT * FROM C WHERE c1 between ? and ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setInt(1, 400);
            ps.setInt(2, 405);
            ResultSet result = ps.executeQuery();
            while(result.next()) {
                result.updateString(2, expectedValue);
                result.updateRow();
            }
        }

        try(PreparedStatement ps = methodWatcher.prepareStatement("SELECT * FROM C WHERE c1 between ? and ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setInt(1, 400);
            ps.setInt(2, 405);
            ResultSet result = ps.executeQuery();
            while(result.next()) {
                Assert.assertEquals(expectedValue, result.getString(2));
            }
        }
    }
}

