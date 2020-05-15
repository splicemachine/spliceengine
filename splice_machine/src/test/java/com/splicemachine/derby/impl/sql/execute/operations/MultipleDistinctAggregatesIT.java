/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 6/8/17.
 */
@Category(LongerThanTwoMinutes.class)
public class MultipleDistinctAggregatesIT extends SpliceUnitTest {
    public static final String CLASS_NAME = MultipleDistinctAggregatesIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, e1 char(3))")
                .withInsert("insert into t1 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1,2,1,1,"aaa"),
                        row(1,2,1,1,"bbb"),
                        row(1,2,2,2,"aaa"),
                        row(1,2,3,3,"bbb"),
                        row(1,1,1,1,"ccc"),
                        row(1,1,1,2,"ddd"),
                        row(null, null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, e3 char(3))")
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 int, d4 int, e4 char(3))")
                .withInsert("insert into t4 values(?,?,?,?,?)")
                .withRows(rows(
                        row(null, null, null, null, null)))
                .create();
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testGroupedMultipleDistinctAggreateViaControlPath() throws Exception {
        /* Q1 */
        String sqlText = "select a1, count(distinct c1), sum(distinct b1) from t1  --splice-properties useSpark=false\n group by a1 order by 1";
        String expected = "A1  | 2 |  3  |\n" +
                "----------------\n" +
                "  1  | 3 |  3  |\n" +
                "NULL | 0 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select a1, sum(distinct c1), sum(distinct b1), max(d1) from t1 --splice-properties useSpark=false\n group by a1 order by 1";
        expected = "A1  |  2  |  3  |  4  |\n" +
                "------------------------\n" +
                "  1  |  6  |  3  |  3  |\n" +
                "NULL |NULL |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select a1, count(distinct b1), count(distinct d1), sum(c1), max(d1) from t1 --splice-properties useSpark=false\n group by a1 order by 1";
        expected = "A1  | 2 | 3 |  4  |  5  |\n" +
                "--------------------------\n" +
                "  1  | 2 | 3 |  9  |  3  |\n" +
                "NULL | 0 | 0 |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select a1, b1, count(distinct c1), sum(d1), count(distinct e1) from t1 --splice-properties useSpark=false\n group by a1, b1 order by a1, b1";
        expected = "A1  | B1  | 3 |  4  | 5 |\n" +
                "--------------------------\n" +
                "  1  |  1  | 1 |  3  | 2 |\n" +
                "  1  |  2  | 3 |  7  | 2 |\n" +
                "NULL |NULL | 0 |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select a1, count(distinct c1), sum(d1), b1, count(distinct e1) from t1 --splice-properties useSpark=false\n group by a1, b1 order by a1, b1";
        expected = "A1  | 2 |  3  | B1  | 5 |\n" +
                "--------------------------\n" +
                "  1  | 1 |  3  |  1  | 2 |\n" +
                "  1  | 3 |  7  |  2  | 2 |\n" +
                "NULL | 0 |NULL |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select a1, sum(distinct c1), sum(distinct b1), max(d1),count(e1) from t1 --splice-properties useSpark=false\n group by a1 order by a1";
        expected = "A1  |  2  |  3  |  4  | 5 |\n" +
                "----------------------------\n" +
                "  1  |  6  |  3  |  3  | 6 |\n" +
                "NULL |NULL |NULL |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 */
        sqlText = "select a1, count(distinct b1), count(distinct c1), count(distinct d1), count(distinct e1) from t1 --splice-properties useSpark=false\n group by a1 order by a1";
        expected = "A1  | 2 | 3 | 4 | 5 |\n" +
                "----------------------\n" +
                "  1  | 2 | 3 | 3 | 4 |\n" +
                "NULL | 0 | 0 | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q8 test empty table */
        sqlText = "select a3, count(distinct b3), sum(distinct c3), count(distinct d3), count(distinct e3), count(*) from t3 --splice-properties useSpark=false\n group by a3 order by a3";
        expected = "";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testGroupedMultipleDistinctAggreateViaSparkPath() throws Exception {
        /* Q1 */
        String sqlText = "select a1, count(distinct c1), sum(distinct b1) from t1  --splice-properties useSpark=true\n group by a1 order by 1";
        String expected = "A1  | 2 |  3  |\n" +
                "----------------\n" +
                "  1  | 3 |  3  |\n" +
                "NULL | 0 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select a1, sum(distinct c1), sum(distinct b1), max(d1) from t1 --splice-properties useSpark=true\n group by a1 order by 1";
        expected = "A1  |  2  |  3  |  4  |\n" +
                "------------------------\n" +
                "  1  |  6  |  3  |  3  |\n" +
                "NULL |NULL |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select a1, count(distinct b1), count(distinct d1), sum(c1), max(d1) from t1 --splice-properties useSpark=true\n group by a1 order by 1";
        expected = "A1  | 2 | 3 |  4  |  5  |\n" +
                "--------------------------\n" +
                "  1  | 2 | 3 |  9  |  3  |\n" +
                "NULL | 0 | 0 |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select a1, b1, count(distinct c1), sum(d1), count(distinct e1) from t1 --splice-properties useSpark=true\n group by a1, b1 order by a1, b1";
        expected = "A1  | B1  | 3 |  4  | 5 |\n" +
                "--------------------------\n" +
                "  1  |  1  | 1 |  3  | 2 |\n" +
                "  1  |  2  | 3 |  7  | 2 |\n" +
                "NULL |NULL | 0 |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select a1, count(distinct c1), sum(d1), b1, count(distinct e1) from t1 --splice-properties useSpark=true\n group by a1, b1 order by a1, b1";
        expected = "A1  | 2 |  3  | B1  | 5 |\n" +
                "--------------------------\n" +
                "  1  | 1 |  3  |  1  | 2 |\n" +
                "  1  | 3 |  7  |  2  | 2 |\n" +
                "NULL | 0 |NULL |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 */
        sqlText = "select a1, sum(distinct c1), sum(distinct b1), max(d1),count(e1) from t1 --splice-properties useSpark=true\n group by a1 order by a1";
        expected = "A1  |  2  |  3  |  4  | 5 |\n" +
                "----------------------------\n" +
                "  1  |  6  |  3  |  3  | 6 |\n" +
                "NULL |NULL |NULL |NULL | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 */
        sqlText = "select a1, count(distinct b1), count(distinct c1), count(distinct d1), count(distinct e1) from t1 --splice-properties useSpark=true\n group by a1 order by a1";
        expected = "A1  | 2 | 3 | 4 | 5 |\n" +
                "----------------------\n" +
                "  1  | 2 | 3 | 3 | 4 |\n" +
                "NULL | 0 | 0 | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q8 test empty table */
        sqlText = "select a3, count(distinct b3), sum(distinct c3), count(distinct d3), count(distinct e3), count(*) from t3 --splice-properties useSpark=true\n group by a3 order by a3";
        expected = "";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testScalarMultipleDistinctAggreateViaControlPath() throws Exception {
        /* Q1 */
        String sqlText = "select count(distinct c1), sum(distinct b1) from t1  --splice-properties useSpark=false";
        String expected = "1 | 2 |\n" +
                "--------\n" +
                " 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select sum(distinct c1), sum(distinct b1), max(d1) from t1 --splice-properties useSpark=false";
        expected = "1 | 2 | 3 |\n" +
                "------------\n" +
                " 6 | 3 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select count(distinct b1), count(distinct d1), sum(c1), max(d1) from t1 --splice-properties useSpark=false";
        expected = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 2 | 3 | 9 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select count(distinct c1), sum(d1), count(distinct e1) from t1 --splice-properties useSpark=false";
        expected = "1 | 2 | 3 |\n" +
                "------------\n" +
                " 3 |10 | 4 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select sum(distinct c1), sum(distinct b1), max(d1),count(e1) from t1 --splice-properties useSpark=false";
        expected = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 6 | 3 | 3 | 6 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 test empty table */
        sqlText = "select count(distinct a3), sum(distinct b3), count(distinct c3), max(distinct d3), count(distinct e3), count(*) from t3 --splice-properties useSpark=false";
        expected = "1 |  2  | 3 |  4  | 5 | 6 |\n" +
                "----------------------------\n" +
                " 0 |NULL | 0 |NULL | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 test null row */
        sqlText = "select count(distinct a4), sum(distinct b4), count(distinct c4), max(distinct d4), count(distinct e4), count(*) from t4 --splice-properties useSpark=false";
        expected = "1 |  2  | 3 |  4  | 5 | 6 |\n" +
                "----------------------------\n" +
                " 0 |NULL | 0 |NULL | 0 | 1 |";
        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testScalarMultipleDistinctAggreateViaSparkPath() throws Exception {
        /* Q1 */
        String sqlText = "select count(distinct c1), sum(distinct b1) from t1  --splice-properties useSpark=true";
        String expected = "1 | 2 |\n" +
                "--------\n" +
                " 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = "select sum(distinct c1), sum(distinct b1), max(d1) from t1 --splice-properties useSpark=true";
        expected = "1 | 2 | 3 |\n" +
                "------------\n" +
                " 6 | 3 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 */
        sqlText = "select count(distinct b1), count(distinct d1), sum(c1), max(d1) from t1 --splice-properties useSpark=true";
        expected = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 2 | 3 | 9 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 */
        sqlText = "select count(distinct c1), sum(d1), count(distinct e1) from t1 --splice-properties useSpark=true";
        expected = "1 | 2 | 3 |\n" +
                "------------\n" +
                " 3 |10 | 4 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q5 */
        sqlText = "select sum(distinct c1), sum(distinct b1), max(d1),count(e1) from t1 --splice-properties useSpark=true";
        expected = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 6 | 3 | 3 | 6 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q6 test empty table */
        sqlText = "select count(distinct a3), sum(distinct b3), count(distinct c3), max(distinct d3), count(distinct e3), count(*) from t3 --splice-properties useSpark=true";
        expected = "1 |  2  | 3 |  4  | 5 | 6 |\n" +
                "----------------------------\n" +
                " 0 |NULL | 0 |NULL | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q7 test null row */
        sqlText = "select count(distinct a4), sum(distinct b4), count(distinct c4), max(distinct d4), count(distinct e4), count(*) from t4 --splice-properties useSpark=true";
        expected = "1 |  2  | 3 |  4  | 5 | 6 |\n" +
                "----------------------------\n" +
                " 0 |NULL | 0 |NULL | 0 | 1 |";
        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }
}
