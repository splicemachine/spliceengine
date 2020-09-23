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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Test automatic casting of CHAR and VARCHAR to numeric types.
 */
@RunWith(Parameterized.class)
@Category(LongerThanTwoMinutes.class)
public class AutoCastIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = AutoCastIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public AutoCastIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (char1 char(50), char2 varchar(50), char3 long varchar, char4 clob, " +
                    "num1 dec(31,0), num2 dec(31,1), num3 dec(31,30), num4 tinyint, num5 smallint, num6 int, num7 bigint," +
                    "num8 numeric, num9 real, num10 float, num11 double)")
                .withInsert("insert into t1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row("1", "1", "1", "1", 1,1,1,1,1,1,1,1,1,1,1),
                        row("-1", "-1", "-1", "-1", -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1)))
                .create();

        new TableCreator(conn)
            .withCreate("create table t2 (char1 char(50), char2 varchar(50), char3 long varchar, char4 clob, " +
                "num1 dec(31,28), num2 dec(10,8), num3 dec(15,8), num4 tinyint, num5 smallint, num6 int, num7 bigint," +
                "num8 real, num9 float, num10 double)")
            .withInsert("insert into t2 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .withRows(rows(
                row("-1.23456789e0", "-1.23456789", "-1.23456789", "-1.23456789",
                    -1.23456789,-1.23456789,-1.23456789,-1,-1,-1,-1,-1.23456789,-1.23456789,-1.23456789)))
            .create();

        new TableCreator(conn)
            .withCreate("create table t3 (char1 char(50), char2 varchar(50), num1 dec(31,31), " +
                "num2 double)")
            .withInsert("insert into t3 values(?,?,?,?)")
            .withRows(rows(
                row("-.1234567890123456789012345678901", "-.1234567890123456789012345678901",
                    -.1234567890123456789012345678901,-.1234567890123456789012345678901)))
            .withIndex("create index autocastit1 on t3(num1)")
            .withIndex("create index autocastit2 on t3(num2)")
            .create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    private void testQuery(String sqlText, String expected) throws Exception {
        ResultSet rs = null;
        try {
            rs = methodWatcher.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }

    private void testQueryFail(String sqlText, String expected) throws Exception {
        ResultSet rs = null;
        try {
            rs = methodWatcher.executeQuery(sqlText);
        }
        catch (SQLException e) {
            Assert.assertTrue(format("\n" + "Expected error code %s.\n", expected), e.getSQLState().equals(expected));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }


    @Test
    public void testSingleTableScan() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |";
        for (int i = 1; i <= 4; i++) {
            for (int j = 1; j <= 11; j++) {
                if (i >= 3) {
                    String errCode = "42818";
                    testQueryFail(format("select 1 from t1 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where char%d=num%d ", useSpark, i, j), errCode);

                    // Reverse the column order in the predicate.
                    testQueryFail(format("select 1 from t1 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where num%d=char%d ", useSpark, j, i), errCode);
                }
                else {
                    testQuery(format("select 1 from t1 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where char%d=num%d ", useSpark, i, j), expected);

                    // Reverse the column order in the predicate.
                    testQuery(format("select 1 from t1 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where num%d=char%d", useSpark, j, i), expected);
                }
            }
        }
    }

    @Test
    public void testJoin() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |";
        for (int i = 1; i <= 4; i++) {
            for (int j = 1; j <= 11; j++) {
                if (i >= 3) {
                    String errCode = "42818";
                    testQueryFail(format("select 1 from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.char%d=tab2.num%d ", useSpark, i, j), errCode);

                    // Reverse the column order in the predicate.
                    testQueryFail(format("select 1 from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.num%d=tab2.char%d", useSpark, j, i), errCode);
                }
                else {
                    testQuery(format("select 1 from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.char%d=tab2.num%d ", useSpark, i, j), expected);

                    // Reverse the column order in the predicate.
                    testQuery(format("select 1 from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.num%d=tab2.char%d", useSpark, j, i), expected);
                }
            }
        }
    }

    @Test
    public void testJoin2() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |";
        String expected2 =
            "";
        for (int i = 1; i <= 4; i++) {
            for (int j = 1; j <= 10; j++) {
                if (i >= 3) {
                    String errCode = "42818";
                    testQueryFail(format("select 1 from t2 tab1, t2 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.char%d=tab2.num%d ", useSpark, i, j), errCode);

                    // Reverse the column order in the predicate.
                    testQueryFail(format("select 1 from t2 tab1, t2 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.num%d=tab2.char%d", useSpark, j, i), errCode);
                }
                else {
                    testQuery(format("select 1 from t2 tab1, t2 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.char%d=tab2.num%d ", useSpark, i, j), j >= 4 && j <= 8 ? expected2 : expected);

                    // Reverse the column order in the predicate.
                    testQuery(format("select 1 from t2 tab1, t2 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                        "where tab1.num%d=tab2.char%d", useSpark, j, i), j >= 4 && j <= 8 ? expected2 : expected);
                }
            }
        }
    }

    @Test
    public void testJoin3() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |";

        for (int i = 1; i <= 2; i++) {
            for (int j = 1; j <= 2; j++) {

                testQuery(format("select 1 from t3 tab1, t3 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                    "where tab1.char%d=tab2.num%d ", useSpark, i, j), expected);

                // Reverse the column order in the predicate.
                testQuery(format("select 1 from t3 tab1, t3 tab2 --SPLICE-PROPERTIES useSpark=%s \n" +
                    "where tab1.num%d=tab2.char%d", useSpark, j, i), expected);
            }
        }
    }

    @Test
    public void testIndexAccess() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |";

        testQuery(format("select 1 from t3 tab1 --SPLICE-PROPERTIES useSpark=%s, index=autocastit1\n" +
            "where tab1.num1='-.1234567890123456789012345678901' ", useSpark), expected);
        testQuery(format("select 1 from t3 tab1 --SPLICE-PROPERTIES useSpark=%s, index=autocastit2\n" +
            "where tab1.num2='-.1234567890123456789012345678901' ", useSpark), expected);
    }

}
