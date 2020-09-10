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
package com.splicemachine.hbase;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.net.InetAddress;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by yxia on 6/28/17.
 */
@Category(LongerThanTwoMinutes.class)
public class DistinctAggregateOverMultiplePartitionsIT extends SpliceUnitTest {
    public static final String CLASS_NAME = DistinctAggregateOverMultiplePartitionsIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(6, 6, 6),
                        row(7, 7, 7),
                        row(8, 8, 8),
                        row(9, 9, 9),
                        row(10, 10, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, e2 varchar(10), constraint con1 primary key (a2))")
                .withInsert("insert into t2 values(?,?,?,?,?)")
                .create();

        /* split the table into multiple partitions */
        spliceClassWatcher.executeUpdate(format("CALL SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS('%s', '%s', null, '%s')",
                CLASS_NAME, "T2", "\\xC2\\x00,\\xC4\\x00,\\xC6\\x00,\\xC8\\x00"));
        spliceClassWatcher.execute("insert into t2 values (1,1,1,1,'aaa'),(2,1,1,1,'bbb'),(3,1,1,2,'ccc')," +
                "(4,1,1,2,'ddd'),(5,1,1,3,'eee'),(6,2,2,3,'fff'),(7,2,2,4,'ggg'), (8,2,2,4,'hhh')," +
                "(9,2,2,5,'iii'),(10,2,2,5,'jjj')");
        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t2 select a2+%d, b2,c2, d2, e2 from t2", factor));
            factor = factor * 2;
        }



        spliceClassWatcher.executeQuery(format("analyze schema %s", CLASS_NAME));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testGroupedMultipleAggregateViaControlPath() throws Exception {
        /* Q1 */
        String sqlText = "select b2, count(distinct c2), sum(distinct a2), count(*) from t2 --splice-properties useSpark=false\n group by b2 order by b2";
        String expected = "B2 | 2 |    3     |  4   |\n" +
                "--------------------------\n" +
                " 1 | 1 |419389440 |20480 |\n" +
                " 2 | 1 |419491840 |20480 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select b2, sum(distinct c2), count(distinct a2), max(d2) from t2 --splice-properties useSpark=false\n group by b2 order by b2";
        expected = "B2 | 2 |  3   | 4 |\n" +
                "-------------------\n" +
                " 1 | 1 |20480 | 3 |\n" +
                " 2 | 2 |20480 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select c2, count(distinct a2), count(distinct d2), sum(b2), max(d2) from t2 --splice-properties useSpark=false\n" +
                  "group by c2 order by c2";
        expected = "C2 |  2   | 3 |  4   | 5 |\n" +
                "--------------------------\n" +
                " 1 |20480 | 3 |20480 | 3 |\n" +
                " 2 |20480 | 3 |40960 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select d2, b2, count(distinct c2), count(distinct e2) from t2 --splice-properties useSpark=false\n" +
                  "group by b2, d2 order by b2, d2";
        expected = "D2 |B2 | 3 | 4 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 2 |\n" +
                " 2 | 1 | 1 | 2 |\n" +
                " 3 | 1 | 1 | 1 |\n" +
                " 3 | 2 | 1 | 1 |\n" +
                " 4 | 2 | 1 | 2 |\n" +
                " 5 | 2 | 1 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select b2, count(distinct c2), sum(d2), b1, count(distinct e2) from t1, t2 --splice-properties useSpark=false\n" +
                  "where b1=b2 group by b1, b2 order by b1, b2";
        expected = "B2 | 2 |  3   |B1 | 5 |\n" +
                "-----------------------\n" +
                " 1 | 1 |36864 | 1 | 5 |\n" +
                " 2 | 1 |86016 | 2 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select e2, sum(distinct c2), sum(distinct b2), max(d2),count(a2) from t2 --splice-properties useSpark=false\n" +
                  "group by e2 order by e2";
        expected = "E2  | 2 | 3 | 4 |  5  |\n" +
                "-----------------------\n" +
                "aaa | 1 | 1 | 1 |4096 |\n" +
                "bbb | 1 | 1 | 1 |4096 |\n" +
                "ccc | 1 | 1 | 2 |4096 |\n" +
                "ddd | 1 | 1 | 2 |4096 |\n" +
                "eee | 1 | 1 | 3 |4096 |\n" +
                "fff | 2 | 2 | 3 |4096 |\n" +
                "ggg | 2 | 2 | 4 |4096 |\n" +
                "hhh | 2 | 2 | 4 |4096 |\n" +
                "iii | 2 | 2 | 5 |4096 |\n" +
                "jjj | 2 | 2 | 5 |4096 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 */
        sqlText = "select b2, count(distinct a2), count(distinct c2), count(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=false\n" +
                  "group by 1, b2 order by b2";
        expected = "B2 |  2   | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                " 1 |20480 | 1 | 3 | 5 |\n" +
                " 2 |20480 | 1 | 3 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q8 */
        sqlText = "select count(distinct a2), sum(distinct b2), e2, sum(distinct c2), max(distinct d2) from t2 --splice-properties useSpark=false\n" +
                  "group by e2 order by e2";
        expected = "1  | 2 |E2  | 4 | 5 |\n" +
                "-----------------------\n" +
                "4096 | 1 |aaa | 1 | 1 |\n" +
                "4096 | 1 |bbb | 1 | 1 |\n" +
                "4096 | 1 |ccc | 1 | 2 |\n" +
                "4096 | 1 |ddd | 1 | 2 |\n" +
                "4096 | 1 |eee | 1 | 3 |\n" +
                "4096 | 2 |fff | 2 | 3 |\n" +
                "4096 | 2 |ggg | 2 | 4 |\n" +
                "4096 | 2 |hhh | 2 | 4 |\n" +
                "4096 | 2 |iii | 2 | 5 |\n" +
                "4096 | 2 |jjj | 2 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testGroupedMultipleAggregateViaSparkPath() throws Exception {
        /* Q1 */
        String sqlText = "select b2, count(distinct c2), sum(distinct a2), count(*) from t2 --splice-properties useSpark=true\n group by b2 order by b2";
        String expected = "B2 | 2 |    3     |  4   |\n" +
                "--------------------------\n" +
                " 1 | 1 |419389440 |20480 |\n" +
                " 2 | 1 |419491840 |20480 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select b2, sum(distinct c2), count(distinct a2), max(d2) from t2 --splice-properties useSpark=true\n group by b2 order by b2";
        expected = "B2 | 2 |  3   | 4 |\n" +
                "-------------------\n" +
                " 1 | 1 |20480 | 3 |\n" +
                " 2 | 2 |20480 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select c2, count(distinct a2), count(distinct d2), sum(b2), max(d2) from t2 --splice-properties useSpark=true\n" +
                "group by c2 order by c2";
        expected = "C2 |  2   | 3 |  4   | 5 |\n" +
                "--------------------------\n" +
                " 1 |20480 | 3 |20480 | 3 |\n" +
                " 2 |20480 | 3 |40960 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select d2, b2, count(distinct c2), count(distinct e2) from t2 --splice-properties useSpark=true\n" +
                "group by b2, d2 order by b2, d2";
        expected = "D2 |B2 | 3 | 4 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 2 |\n" +
                " 2 | 1 | 1 | 2 |\n" +
                " 3 | 1 | 1 | 1 |\n" +
                " 3 | 2 | 1 | 1 |\n" +
                " 4 | 2 | 1 | 2 |\n" +
                " 5 | 2 | 1 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select b2, count(distinct c2), sum(d2), b1, count(distinct e2) from t1, t2 --splice-properties useSpark=true\n" +
                "where b1=b2 group by b1, b2 order by b1, b2";
        expected = "B2 | 2 |  3   |B1 | 5 |\n" +
                "-----------------------\n" +
                " 1 | 1 |36864 | 1 | 5 |\n" +
                " 2 | 1 |86016 | 2 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select e2, sum(distinct c2), sum(distinct b2), max(d2),count(a2) from t2 --splice-properties useSpark=true\n" +
                "group by e2 order by e2";
        expected = "E2  | 2 | 3 | 4 |  5  |\n" +
                "-----------------------\n" +
                "aaa | 1 | 1 | 1 |4096 |\n" +
                "bbb | 1 | 1 | 1 |4096 |\n" +
                "ccc | 1 | 1 | 2 |4096 |\n" +
                "ddd | 1 | 1 | 2 |4096 |\n" +
                "eee | 1 | 1 | 3 |4096 |\n" +
                "fff | 2 | 2 | 3 |4096 |\n" +
                "ggg | 2 | 2 | 4 |4096 |\n" +
                "hhh | 2 | 2 | 4 |4096 |\n" +
                "iii | 2 | 2 | 5 |4096 |\n" +
                "jjj | 2 | 2 | 5 |4096 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 */
        sqlText = "select b2, count(distinct a2), count(distinct c2), count(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=true\n" +
                "group by 1, b2 order by b2";
        expected = "B2 |  2   | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                " 1 |20480 | 1 | 3 | 5 |\n" +
                " 2 |20480 | 1 | 3 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q8 */
        sqlText = "select count(distinct a2), sum(distinct b2), e2, sum(distinct c2), max(distinct d2) from t2 --splice-properties useSpark=true\n" +
                "group by e2 order by e2";
        expected = "1  | 2 |E2  | 4 | 5 |\n" +
                "-----------------------\n" +
                "4096 | 1 |aaa | 1 | 1 |\n" +
                "4096 | 1 |bbb | 1 | 1 |\n" +
                "4096 | 1 |ccc | 1 | 2 |\n" +
                "4096 | 1 |ddd | 1 | 2 |\n" +
                "4096 | 1 |eee | 1 | 3 |\n" +
                "4096 | 2 |fff | 2 | 3 |\n" +
                "4096 | 2 |ggg | 2 | 4 |\n" +
                "4096 | 2 |hhh | 2 | 4 |\n" +
                "4096 | 2 |iii | 2 | 5 |\n" +
                "4096 | 2 |jjj | 2 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testScalarMultipleAggregateViaControlPath() throws Exception {
        /* Q1 */
        String sqlText = "select count(distinct c2), sum(distinct a2), count(*) from t2 --splice-properties useSpark=false";
        String expected = "1 |    2     |  3   |\n" +
                "----------------------\n" +
                " 2 |838881280 |40960 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select count(distinct a2), count(distinct d2), sum(b2), max(d2) from t2 --splice-properties useSpark=false";
        expected = "1   | 2 |  3   | 4 |\n" +
                "----------------------\n" +
                "40960 | 5 |61440 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select count(distinct c2), sum(d2), count(distinct e2) from t1, t2 --splice-properties useSpark=false\n where b1=b2";
        expected = "1 |   2   | 3 |\n" +
                "----------------\n" +
                " 2 |122880 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select count(distinct a2), count(distinct b2), count(distinct c2), count(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=false";
        expected = "1   | 2 | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                "40960 | 2 | 2 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select count(distinct a2), sum(distinct b2), sum(distinct c2), max(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=false";
        expected = "1   | 2 | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                "40960 | 3 | 3 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select count(a2), avg(cast(b2 as float)), sum(distinct c2), max(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=false";
        expected = "1   | 2  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "40960 |1.5 | 3 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testScalarMultipleAggregateViaSparkPath() throws Exception {
        /* Q1 */
        String sqlText = "select count(distinct c2), sum(distinct a2), count(*) from t2 --splice-properties useSpark=true";
        String expected = "1 |    2     |  3   |\n" +
                "----------------------\n" +
                " 2 |838881280 |40960 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select count(distinct a2), count(distinct d2), sum(b2), max(d2) from t2 --splice-properties useSpark=true";
        expected = "1   | 2 |  3   | 4 |\n" +
                "----------------------\n" +
                "40960 | 5 |61440 | 5 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select count(distinct c2), sum(d2), count(distinct e2) from t1, t2 --splice-properties useSpark=true\n where b1=b2";
        expected = "1 |   2   | 3 |\n" +
                "----------------\n" +
                " 2 |122880 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select count(distinct a2), count(distinct b2), count(distinct c2), count(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=true";
        expected = "1   | 2 | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                "40960 | 2 | 2 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select count(distinct a2), sum(distinct b2), sum(distinct c2), max(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=true";
        expected = "1   | 2 | 3 | 4 | 5 |\n" +
                "-----------------------\n" +
                "40960 | 3 | 3 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select count(a2), avg(cast(b2 as float)), sum(distinct c2), max(distinct d2), count(distinct e2) from t2 --splice-properties useSpark=true";
        expected = "1   | 2  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "40960 |1.5 | 3 | 5 |10 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    @Ignore("SPLICE-1924")
    public void testSplitsSparkProperty() throws Exception {
        TestConnection c = methodWatcher.createConnection();
        ResultSet rs = c.createStatement().executeQuery("CALL SYSCS_UTIL.SYSCS_GET_SESSION_INFO()");
        assertTrue(rs.next());
        String host = rs.getString(1);
        int session = rs.getInt(2);
        String id = host + "," + session;
        rs.close();

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String localHostname = InetAddress.getLocalHost().getHostName();

        URL url = new URL("http://" + localHostname + ":4040/api/v1/applications");
        List<SparkApp> apps = mapper.readValue(url, new TypeReference<List<SparkApp>>(){});
        SparkApp app = apps.get(0);

        String sql = "select count(*) from " + spliceSchemaWatcher.schemaName + ".t2 --splice-properties useSpark=true";

        rs = c.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        rs.close();


        url = new URL(url.toExternalForm() +"/"+app.id+"/jobs");
        List<SparkJob> jobs =  mapper.readValue(url, new TypeReference<List<SparkJob>>(){});
        int initialTasks = -1;
        int splitsTasks = -1;

        for (SparkJob job : jobs) {
            if (job.jobGroup.contains(id)) {
                initialTasks = job.numTasks;
            }
        }

        assertTrue("Job not found",initialTasks > 0);

        rs = c.createStatement().executeQuery(sql + ", splits=100");
        assertTrue(rs.next());
        rs.close();

        jobs =  mapper.readValue(url, new TypeReference<List<SparkJob>>(){});
        for (SparkJob job : jobs) {
            if (job.jobGroup.contains(id) && job.numTasks > initialTasks) {
                splitsTasks = job.numTasks;
            }
        }

        assertTrue("Job not found",splitsTasks > initialTasks + 10);
    }

    public static class SparkJob {
        private  String jobGroup;
        private int numTasks;

        public String getJobGroup() {
            return jobGroup;
        }

        public void setJobGroup(String jobGroup) {
            this.jobGroup = jobGroup;
        }

        public int getNumTasks() {
            return numTasks;
        }

        public void setNumTasks(int numTasks) {
            this.numTasks = numTasks;
        }
    }

    public static class SparkApp {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}
