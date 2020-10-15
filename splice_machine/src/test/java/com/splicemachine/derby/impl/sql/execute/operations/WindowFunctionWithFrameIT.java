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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import com.splicemachine.utils.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 8/27/20.
 */
@RunWith(Parameterized.class)
public class WindowFunctionWithFrameIT extends SpliceUnitTest {
    public static final String CLASS_NAME = WindowFunctionWithFrameIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private String useSpark;


    public WindowFunctionWithFrameIT(String useSpark) {
        this.useSpark = useSpark;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int , b1 int)")
                .withInsert("insert into t1 values(?,?)")
                .withRows(rows(
                        row(1,1),
                        row(2,1),
                        row(3,1),
                        row(4,1),
                        row(5,1),
                        row(10,2),
                        row(20,2)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testSumForFrameWindowFallBeforeCurrentRow() throws Exception {
        String[] expected = {
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |  NULL   |\n" +
                        " 1 | 3 |  NULL   |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |    1    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    3    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |    7    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |    1    |\n" +
                        " 1 | 2 |    3    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |    7    |\n" +
                        " 1 | 5 |    9    |\n" +
                        " 2 |10 |   10    |\n" +
                        " 2 |20 |   30    |"
        };

        List<Pair<Integer, Integer>> windows = new ArrayList<>();
        windows.add(new Pair(1,1));
        windows.add(new Pair(4, 4));
        windows.add(new Pair(2,1));
        windows.add(new Pair(1,0));

        int i = 0;
        for (Pair<Integer, Integer> pair: windows) {
            String sqlText =
                    String.format("select b1, a1, sum(a1) over (partition by b1 order by a1 ROWS BETWEEN %d PRECEDING AND %d PRECEDING) as win_sum\n" +
                            "from t1 --splice-properties useSpark=%s\n" +
                            "order by b1, a1", pair.getFirst(), pair.getSecond(), useSpark);

            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals(format("\nPair(%d, %d):" + sqlText + "\n", pair.getFirst(), pair.getSecond()), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            i++;
        }
    }

    @Test
    public void testSumForFrameWindowFallAfterCurrentRow() throws Exception {
        String[] expected = {
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    3    |\n" +
                        " 1 | 3 |    4    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |    5    |\n" +
                        " 1 | 2 |  NULL   |\n" +
                        " 1 | 3 |  NULL   |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |    5    |\n" +
                        " 1 | 2 |    7    |\n" +
                        " 1 | 3 |    9    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 | WIN_SUM |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    5    |\n" +
                        " 1 | 3 |    7    |\n" +
                        " 1 | 4 |    9    |\n" +
                        " 1 | 5 |    5    |\n" +
                        " 2 |10 |   30    |\n" +
                        " 2 |20 |   20    |"
        };

        List<Pair<Integer, Integer>> windows = new ArrayList<>();
        windows.add(new Pair(1,1));
        windows.add(new Pair(4, 4));
        windows.add(new Pair(1,2));
        windows.add(new Pair(0,1));

        int i = 0;
        for (Pair<Integer, Integer> pair: windows) {
            String sqlText =
                    String.format("select b1, a1, sum(a1) over (partition by b1 order by a1 ROWS BETWEEN %d FOLLOWING AND %d FOLLOWING) as win_sum\n" +
                            "from t1 --splice-properties useSpark=%s\n" +
                            "order by b1, a1", pair.getFirst(), pair.getSecond(), useSpark);

            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals(format("\nPair(%d, %d):" + sqlText + "\n", pair.getFirst(), pair.getSecond()), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            i++;
        }
    }

    @Test
    public void testSumForFrameWindowCoversCurrentRow() throws Exception {
        String expected = "B1 |A1 | WIN_SUM |\n" +
                "------------------\n" +
                " 1 | 1 |    3    |\n" +
                " 1 | 2 |    6    |\n" +
                " 1 | 3 |    9    |\n" +
                " 1 | 4 |   12    |\n" +
                " 1 | 5 |    9    |\n" +
                " 2 |10 |   30    |\n" +
                " 2 |20 |   30    |";
        String sqlText =
                String.format("select b1, a1, sum(a1) over (partition by b1 order by a1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as win_sum\n" +
                        "from t1 --splice-properties useSpark=%s\n" +
                        "order by b1, a1", useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testVariousFuncForFrameWindowFallBeforeCurrentRow() throws Exception {
        String[] expected = {
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    0    |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    2    |\n" +
                        " 2 |10 |    0    |\n" +
                        " 2 |20 |    1    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    0    |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    2    |\n" +
                        " 2 |10 |    0    |\n" +
                        " 2 |20 |    1    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |"
        };

        ArrayList<String> functions = new ArrayList<>();
        functions.add("AVG(a1)");
        functions.add("COUNT(*)");
        functions.add("COUNT(a1)");
        functions.add("FIRST_VALUE(a1)");
        functions.add("LAST_VALUE(a1)");
        functions.add("MIN(a1)");
        functions.add("MAX(a1)");

        int i = 0;
        for (String func: functions) {
            String sqlText =
                    String.format("select b1, a1, %s over (partition by b1 order by a1 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) as win_func\n" +
                            "from t1 --splice-properties useSpark=%s\n" +
                            "order by b1, a1", func, useSpark);

            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals(format("\nFunction: %s" + sqlText + "\n", func), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            i++;
        }
    }

    @Test
    public void testVariousFuncForFrameWindowFallAfterCurrentRow() throws Exception {
        String[] expected = {
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    2    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    0    |\n" +
                        " 1 | 5 |    0    |\n" +
                        " 2 |10 |    0    |\n" +
                        " 2 |20 |    0    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    2    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    0    |\n" +
                        " 1 | 5 |    0    |\n" +
                        " 2 |10 |    0    |\n" +
                        " 2 |20 |    0    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    4    |\n" +
                        " 1 | 2 |    5    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    4    |\n" +
                        " 1 | 2 |    5    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |"
        };

        ArrayList<String> functions = new ArrayList<>();
        functions.add("AVG(a1)");
        functions.add("COUNT(*)");
        functions.add("COUNT(a1)");
        functions.add("FIRST_VALUE(a1)");
        functions.add("LAST_VALUE(a1)");
        functions.add("MIN(a1)");
        functions.add("MAX(a1)");

        int i = 0;
        for (String func: functions) {
            String sqlText =
                    String.format("select b1, a1, %s over (partition by b1 order by a1 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) as win_func\n" +
                            "from t1 --splice-properties useSpark=%s\n" +
                            "order by b1, a1", func, useSpark);

            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals(format("\nFunction: %s" + sqlText + "\n", func), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            i++;
        }
    }

    @Test
    public void testVariousFuncForFrameWindowCoverCurrentRow() throws Exception {
        String[] expected = {
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    2    |\n" +
                        " 1 | 3 |    3    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |   15    |\n" +
                        " 2 |20 |   15    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |    4    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |    2    |\n" +
                        " 2 |20 |    2    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |    4    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |    2    |\n" +
                        " 2 |20 |    2    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    1    |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |   10    |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |    5    |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |   20    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    1    |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |   10    |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |    5    |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |   20    |"
        };

        ArrayList<String> functions = new ArrayList<>();
        functions.add("AVG(a1)");
        functions.add("COUNT(*)");
        functions.add("COUNT(a1)");
        functions.add("FIRST_VALUE(a1)");
        functions.add("LAST_VALUE(a1)");
        functions.add("MIN(a1)");
        functions.add("MAX(a1)");

        int i = 0;
        for (String func: functions) {
            String sqlText =
                    String.format("select b1, a1, %s over (partition by b1 order by a1 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as win_func\n" +
                            "from t1 --splice-properties useSpark=%s\n" +
                            "order by b1, a1", func, useSpark);

            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals(format("\nFunction: %s" + sqlText + "\n", func), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            i++;
        }
    }

    @Test
    public void testLeadLagForDifferentFrameWindows() throws Exception {
        String[] expected = {
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    3    |\n" +
                        " 1 | 3 |    4    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |"
        };

        List<Pair<Pair<Integer, String>, Pair<Integer, String>>> windows = new ArrayList<>();
        windows.add(new Pair(new Pair(1,"Preceding"), new Pair(1, "Preceding")));
        windows.add(new Pair(new Pair(3,"Preceding"), new Pair(2, "Preceding")));
        windows.add(new Pair(new Pair(1,"Preceding"), new Pair(1, "Following")));
        windows.add(new Pair(new Pair(1,"Following"), new Pair(1, "Following")));
        windows.add(new Pair(new Pair(1,"Following"), new Pair(2, "Following")));

        ArrayList<String> functions = new ArrayList<>();
        functions.add("LEAD(a1)");
        functions.add("LAG(a1)");
        int i = 0;
        for (String func: functions) {
            for (Pair<Pair<Integer, String>, Pair<Integer, String>> pair : windows) {
                String sqlText =
                        String.format("select b1, a1, %s over (partition by b1 order by a1 ROWS BETWEEN %d %s AND %d %s) as win_func\n" +
                                        "from t1 --splice-properties useSpark=%s\n" +
                                        "order by b1, a1", func,
                                pair.getFirst().getFirst(), pair.getFirst().getSecond(),
                                pair.getSecond().getFirst(), pair.getSecond().getSecond(), useSpark);

                ResultSet rs = methodWatcher.executeQuery(sqlText);

                assertEquals(format("\nFunction: %s: Pair(%d, %s, %d, %s):" + sqlText + "\n",
                        func, pair.getFirst().getFirst(), pair.getFirst().getSecond(),
                        pair.getSecond().getFirst(), pair.getSecond().getSecond()), expected[i], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
                rs.close();
            }
            i++;
        }
    }

    @Test
    public void testLeadLagForDifferentOffset() throws Exception {
        String[] expected = {
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    2    |\n" +
                        " 1 | 2 |    3    |\n" +
                        " 1 | 3 |    4    |\n" +
                        " 1 | 4 |    5    |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |   20    |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |    3    |\n" +
                        " 1 | 2 |    4    |\n" +
                        " 1 | 3 |    5    |\n" +
                        " 1 | 4 |  NULL   |\n" +
                        " 1 | 5 |  NULL   |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |    1    |\n" +
                        " 1 | 3 |    2    |\n" +
                        " 1 | 4 |    3    |\n" +
                        " 1 | 5 |    4    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |   10    |",
                "B1 |A1 |WIN_FUNC |\n" +
                        "------------------\n" +
                        " 1 | 1 |  NULL   |\n" +
                        " 1 | 2 |  NULL   |\n" +
                        " 1 | 3 |    1    |\n" +
                        " 1 | 4 |    2    |\n" +
                        " 1 | 5 |    3    |\n" +
                        " 2 |10 |  NULL   |\n" +
                        " 2 |20 |  NULL   |"
        };

        ArrayList<String> functions = new ArrayList<>();
        functions.add("LEAD");
        functions.add("LAG");
        int i = 0;
        for (String func: functions) {
            for (int j=1; j<3; j++) {
                String sqlText =
                        String.format("select b1, a1, %s(a1, %d) over (partition by b1 order by a1) as win_func\n" +
                                        "from t1 --splice-properties useSpark=%s\n" +
                                        "order by b1, a1", func, j, useSpark);

                ResultSet rs = methodWatcher.executeQuery(sqlText);

                assertEquals(format("\nFunction: %s(a1, %d):" + sqlText + "\n", func, j), expected[i*2+j-1], TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
                rs.close();
            }
            i++;
        }
    }
}
