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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Test native spark mergesort join with inequality join conditions.
 */
@RunWith(Parameterized.class)
@Category(LongerThanTwoMinutes.class)
public class NativeSparkJoinWithInequalityPredsIT  extends SpliceUnitTest {

    private String joinStrategy;
    private String useSpark;
    private static final String[] joins = {", ", "where not exists (select 1 from "};
    private static final String[] whereOrOnClause = {"where", "where"};
    private static final String[] closingParenthesis = {"", ")"};

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(1);
        params.add(new Object[]{"BROADCAST","true"});
        return params;
    }

    public static final String CLASS_NAME = NativeSparkJoinWithInequalityPredsIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public NativeSparkJoinWithInequalityPredsIT(String joinStrategy, String useSpark) {
        this.joinStrategy = joinStrategy;
        this.useSpark     = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table ts_int (j int, t tinyint, s smallint, i int, l bigint, d decimal(38,0))")
                .create();

        String [] values = {
        "(1, 127, 32767, 2147483647, 9223372036854775807, 9223372036854775807)",
        "(1, 126, 32766, 2147483646, 9223372036854775806, 9223372036854775806)",
        "(1, 126, 32766, 2147483646, 9223372036854775806, 9223372036854775806)",
        "(1, 125, 32765, 2147483645, 9223372036854775805, 9223372036854775805)",
        "(1, -128, -32768, -2147483648, -9223372036854775808, -9223372036854775808)",
        "(1, -127, -32767, -2147483647, -9223372036854775807, -9223372036854775807)",
        "(1, null, null, null, null, null)"
        };
        for (int i=0; i < values.length; i++) {
            spliceClassWatcher.executeUpdate(format("insert into ts_int values %s", values[i]));
        }

        new TableCreator(conn)
                .withCreate("create table ts_float (j double, f1 float(40), f2 float(45), f3 real, f4 double)")
                .withInsert("insert into ts_float values(?, ?, ?, ?, ?)")
                .create();

        String [] values2 = {
        "(1, 3.11111111111E+8, 3.111111111111111111E+8, 3.11111111111E+8, 3.111111111111111111E+8)",
        "(1, 3.11111111111E+28, 3.111111111111111111E+3, 3.11111111111E+8, 3.111111111111111111E+3)",
        "(1, 3.11111111111E+18, 3.111111111111111111E+13, 3.11111111111E+8, 3.111111111111111111E+13)",
        "(1, 3.11111111111E+54, 3.111111111111111111E+72, 3.11111111111E+8, 3.111111111111111111E+72)",
        "(1, null, null, null, null)"
        };

        for (int i=0; i < values2.length; i++) {
            spliceClassWatcher.executeUpdate(format("insert into ts_float values %s", values2[i]));
        }

        new TableCreator(conn)
                .withCreate("create table ts_datetime (j date, d date, ts timestamp, t time)")
                .withInsert("insert into ts_datetime values(?, ?, ?, ?)")
                .withRows(rows(
                        row("1999-01-01", "1999-01-01", "1999-01-01 12:00:00", "12:00:00"),
                        row("1999-01-01", "1999-01-02", "1999-01-02 12:00:00", "12:01:00"),
                        row("1999-01-01", "1999-01-03", "1999-01-03 12:00:00", "12:03:00"),
                        row("1999-01-01", "1999-01-04", "1999-01-04 12:00:00", "11:00:00"),
                        row("1999-01-01", null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_char (j char(1), b char(3), c varchar(2), d long varchar)")
                .withInsert("insert into ts_char values(?, ?, ?, ?)")
                .withRows(rows(
                        row("1", "1", "1","1"),
                        row("1", "2", "2","2"),
                        row("1", "3", "3","3"),
                        row("1", "4", "4","4"),
                        row("1", null, null, null)))
                .create();

    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testInt() throws Exception {

        /* create table ts_int (j int, t tinyint, s smallint, i int, l bigint, d decimal(38,0)) */
        String sqlText = null;
        String expected = null;
        String expectedTemplate =
                "1 |\n" +
                "----\n" +
                "%s |";

        String [][] expectedCounts = {{"14","24"},
                                      {" 2"," 3"}};

        String [] joinConditions = {"%s tab1.j = tab2.j and tab1.t > tab2.t ",
                                    "%s tab1.j = tab2.j and ((tab1.t > tab2.t or tab1.t > tab2.t * 0.9) and tab1.t > tab2.t / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions.length; j++) {
            sqlText = format("/* i=%d, j=%d */ select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", i, j, joins[i], useSpark, joinStrategy, joinConditions[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

       String []  joinConditions2 = {"%s tab1.j = tab2.j and tab1.s > tab2.s ",
                                    "%s tab1.j = tab2.j and ((tab1.s > tab2.s or tab1.s > tab2.s * 0.9) and tab1.s > tab2.s / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions2.length; j++) {
            sqlText = format("select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", joins[i], useSpark, joinStrategy, joinConditions2[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

       String []  joinConditions3 = {"%s tab1.j = tab2.j and tab1.i > tab2.i ",
                                    "%s tab1.j = tab2.j and ((tab1.i > tab2.i or tab1.i > tab2.i * 0.9) and tab1.i > tab2.i / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions3.length; j++) {
            sqlText = format("select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", joins[i], useSpark, joinStrategy, joinConditions3[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

       String []  joinConditions4 = {"%s tab1.j = tab2.j and tab1.l > tab2.l ",
                                    "%s tab1.j = tab2.j and ((tab1.l > tab2.l or tab1.l > tab2.l * 0.9) and tab1.l > tab2.l / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions4.length; j++) {
            sqlText = format("select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", joins[i], useSpark, joinStrategy, joinConditions4[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

       String []  joinConditions5 = {"%s tab1.j = tab2.j and tab1.d > tab2.d ",
                                    "%s tab1.j = tab2.j and ((tab1.d > tab2.d or tab1.d > tab2.d * 0.9) and tab1.d > tab2.d / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions5.length; j++) {
            sqlText = format("select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", joins[i], useSpark, joinStrategy, joinConditions5[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

       String []  joinConditions6 = {"%s tab1.j = tab2.j and tab1.d > tab2.l ",
                                    "%s tab1.j = tab2.j and ((tab1.d > tab2.l or tab1.d > tab2.l * 0.9) and tab1.l > tab2.d / 1.1) "
        };
        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions6.length; j++) {
            sqlText = format("select count(*) from ts_int tab1 %s ts_int tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", joins[i], useSpark, joinStrategy, joinConditions6[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }
    }

    @Test
    public void testFloat() throws Exception {

        /* create table ts_float (j double, f1 float(40), f2 float(45), f3 real, f4 double) */
        String sqlText = null;
        String expected = null;
        String expectedTemplate =
                "1 |\n" +
                "----\n" +
                "%s |";

        String [][] expectedCounts = {{" 6"," 6"},
                                      {" 2"," 2"}};

        String [] joinConditions = {"%s tab1.j = tab2.j and tab1.f1 > tab2.f1 ",
                                    "%s tab1.j = tab2.j and ((tab1.f1 > tab2.f1 or tab1.f1 > tab2.f1 - 1) and tab1.f1 > tab2.f1 + 1) "};

        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions.length; j++) {
            sqlText = format("/* i=%d, j=%d */ select count(*) from ts_float tab1 %s ts_float tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s"
            , i, j, joins[i], useSpark, joinStrategy, joinConditions[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts[i][j]);
            testQueryUnsorted(sqlText, expected, methodWatcher);
          }

        String [] joinConditions2 = {"%s tab1.j = tab2.j and tab1.f2 > tab2.f2 ",
                                    "%s tab1.j = tab2.j and ((tab1.f2 > tab2.f2 or tab1.f2 > tab2.f2 - 1) and tab1.f2 > tab2.f2 + 1) "};
        String [][] expectedCounts2 = {{" 6"," 6"},
                                      {" 2"," 2"}};

        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions2.length; j++) {
            sqlText = format("/* i=%d, j=%d */ select count(*) from ts_float tab1 %s ts_float tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", i, j, joins[i], useSpark, joinStrategy, joinConditions2[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts2[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

        String [] joinConditions3 = {"%s tab1.j = tab2.j and tab1.f3 > tab2.f3 ",
                                    "%s tab1.j = tab2.j and ((tab1.f3 > tab2.f3 or tab1.f3 > tab2.f3 - 1) and tab1.f3 > tab2.f3 + 1) "};

        String [][] expectedCounts3 = {{" 0"," 0"},
                                      {" 5"," 5"}};

        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions3.length; j++) {
            sqlText = format("/* i=%d, j=%d */ select count(*) from ts_float tab1 %s ts_float tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", i, j, joins[i], useSpark, joinStrategy, joinConditions3[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts3[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }

        String [] joinConditions4 = {"%s tab1.j = tab2.j and tab1.f4 > tab2.f4 ",
                                    "%s tab1.j = tab2.j and ((tab1.f4 > tab2.f4 or tab1.f4 > tab2.f4 - 1) and tab1.f4 > tab2.f4 + 1) "};

        String [][] expectedCounts4 = {{" 6"," 6"},
                                      {" 2"," 2"}};

        for (int i = 0; i < joins.length; i++)
          for (int j = 0; j < joinConditions4.length; j++) {
            sqlText = format("/* i=%d, j=%d */ select count(*) from ts_float tab1 %s ts_float tab2 " +
            "--splice-properties useSpark=%s, joinStrategy=%s\n" +
            "%s " +
            "%s" +
            "order by 1", i, j, joins[i], useSpark, joinStrategy, joinConditions4[j], closingParenthesis[i]);
            sqlText = format(sqlText, whereOrOnClause[i]);
            expected = format(expectedTemplate, expectedCounts4[i][j]);
            testQuery(sqlText, expected, methodWatcher);
          }
    }

    @Test
    public void testDateTime() throws Exception {

        /* create table ts_datetime (j date, d date, t timestamp) */
        String sqlText = null;
        String expected = null;
        String expectedTemplate =
        "1 |\n" +
        "----\n" +
        "%s |";

        String[][] expectedCounts = {{" 6"," 3"},
                                    {" 2"," 3"}};

        String[] joinConditions = {"%s tab1.j = tab2.j and tab1.d > tab2.d ",
        "%s tab1.j = tab2.j and ((tab1.d > tab2.d or tab1.d > tab2.d - 1) and tab1.d > tab2.d + 1) "};

        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_datetime tab1 %s ts_datetime tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, joinStrategy, joinConditions[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }

        String[][] expectedCounts2 = {{" 6"," 3"},
                                    {" 2", " 3"}};

        String[] joinConditions2 = {"%s tab1.j = tab2.j and tab1.ts > tab2.ts ",
        "%s tab1.j = tab2.j and ((tab1.ts > tab2.ts or tab1.ts > tab2.ts - 1) and tab1.ts > tab2.ts + 1) "};

        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions2.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_datetime tab1 %s ts_datetime tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, joinStrategy, joinConditions2[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts2[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }

        String[][] expectedCounts3 = {{" 6"," 3"},
                                    {" 2"," 2"}};

        String[] joinConditions3 = {"%s tab1.j = tab2.j and tab1.t > tab2.t ",
        "%s tab1.j = tab2.j and ((tab1.t = tab2.t or tab1.t is null) and tab1.t > TO_TIME('11:44', 'HH:mm')) ",
        };

        /* for the scenario i=3, j=1, the not exists subquery is not flattened, so broadcast join is not applicable,
        use nestedloop join instead which is the only applicable join strategy */
        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions3.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_datetime tab1 %s ts_datetime tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, (j==1?"nestedloop" : joinStrategy), joinConditions3[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts3[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }
    }

    @Test
    public void testChar() throws Exception {

        /* create table ts_char (j char(1), b char(3), c varchar(2), d long varchar) */
        String sqlText = null;
        String expected = null;
        String expectedTemplate =
        "1 |\n" +
        "----\n" +
        "%s |";

        String[][] expectedCounts = {{" 6"," 3"},
                                    {" 2"," 3"}};

        String[] joinConditions = {"%s tab1.j = tab2.j and tab1.b > tab2.b ",
        "%s tab1.j = tab2.j and ((tab1.b > tab2.b or tab1.b > tab2.b - 1) and tab1.b > tab2.b + 1) "};

        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_char tab1 %s ts_char tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, joinStrategy, joinConditions[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }

        String[][] expectedCounts2 = {{" 6"," 3"},
                                    {" 2"," 3"}};

        String[] joinConditions2 = {"%s tab1.j = tab2.j and tab1.c > tab2.c ",
        "%s tab1.j = tab2.j and ((tab1.c > tab2.c or tab1.c > tab2.c - 1) and tab1.c > tab2.c + 1) "};

        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions2.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_char tab1 %s ts_char tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, joinStrategy, joinConditions2[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts2[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }

        String[][] expectedCounts3 = {{"10"," 3"},
                                    {" 1"," 3"}};

        String[] joinConditions3 = {"%s tab1.j = tab2.j and tab1.b > tab2.c ",
        "%s tab1.j = tab2.j and ((tab1.b > tab2.c or tab1.b > tab2.c - 1) and tab1.b > tab2.c + 1) "};

        for (int i = 0; i < joins.length; i++)
            for (int j = 0; j < joinConditions3.length; j++) {
                sqlText = format("/* i=%d, j=%d */ select count(*) from ts_char tab1 %s ts_char tab2 " +
                "--splice-properties useSpark=%s, joinStrategy=%s\n" +
                "%s " +
                "%s"
                , i, j, joins[i], useSpark, joinStrategy, joinConditions3[j], closingParenthesis[i]);
                sqlText = format(sqlText, whereOrOnClause[i]);
                expected = format(expectedTemplate, expectedCounts3[i][j]);
                testQueryUnsorted(sqlText, expected, methodWatcher);
            }
    }
}
