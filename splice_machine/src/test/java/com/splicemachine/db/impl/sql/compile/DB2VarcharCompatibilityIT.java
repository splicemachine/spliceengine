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
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Test DB2 Varchar compatibility mode (ignore trailing spaces in comparisons).
 */
@Category({SerialTest.class})
@RunWith(Parameterized.class)
public class DB2VarcharCompatibilityIT extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = DB2VarcharCompatibilityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public DB2VarcharCompatibilityIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a int, b varchar(10), primary key(b))")
                .withInsert("insert into t1 values(?, ?)")
                .withRows(rows(
                        row(1, "a"),
                        row(1, "a "),
                        row(1, "a  "),
                        row(1, "a   "),
                        row(1, "ab"),
                        row(1, "abc"),
                        row(1, "a b"),
                        row(1, "abcd")))
                .create();

        new TableCreator(conn)
                .withCreate("create table t11 (a int, b varchar(10))")
                .create();

        String sqlText = "insert into t11 select * from t1";
		spliceClassWatcher.executeUpdate(sqlText);
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
        spliceClassWatcher.executeUpdate("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', true)");
    }

    @AfterClass
    public static void exitDB2CompatibilityMode() throws Exception {
        spliceClassWatcher.executeUpdate("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', null)");
    }

    @Test
    public void testInnerJoin() throws Exception {
        String sqlTemplate = "select * from t1 a --splice-properties useSpark=" + useSpark.toString() +
                             ", joinStrategy=%s\n, t1 b where a.b = b.b";
        String sqlTemplate2 = "select * from t11 a --splice-properties useSpark=" + useSpark.toString() +
                             ", joinStrategy=%s\n, t11 b --splice-properties joinStrategy=%s\n where a.b = b.b";
        String sqlText = format(sqlTemplate, "BROADCAST");

        String expected =
            "A |  B  | A |  B  |\n" +
            "--------------------\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 | a b | 1 | a b |\n" +
            " 1 | ab  | 1 | ab  |\n" +
            " 1 | abc | 1 | abc |\n" +
            " 1 |abcd | 1 |abcd |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "MERGE");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate2, "NESTEDLOOP", "NESTEDLOOP");
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testInequalityJoin() throws Exception {
        String sqlTemplate = "select * from t1 a --splice-properties useSpark=" + useSpark.toString() +
                             ", joinStrategy=%s\n, t1 b where a.b > b.b";
        String sqlTemplate2 = "select * from t11 a --splice-properties useSpark=" + useSpark.toString() +
                             ", joinStrategy=%s\n, t11 b --splice-properties joinStrategy=%s\n where a.b > b.b";
        String sqlText = format(sqlTemplate, "BROADCAST");

        String expected =
            "A |  B  | A | B  |\n" +
            "-------------------\n" +
            " 1 | a b | 1 | a  |\n" +
            " 1 | a b | 1 | a  |\n" +
            " 1 | a b | 1 | a  |\n" +
            " 1 | a b | 1 | a  |\n" +
            " 1 | ab  | 1 | a  |\n" +
            " 1 | ab  | 1 | a  |\n" +
            " 1 | ab  | 1 | a  |\n" +
            " 1 | ab  | 1 | a  |\n" +
            " 1 | ab  | 1 |a b |\n" +
            " 1 | abc | 1 | a  |\n" +
            " 1 | abc | 1 | a  |\n" +
            " 1 | abc | 1 | a  |\n" +
            " 1 | abc | 1 | a  |\n" +
            " 1 | abc | 1 |a b |\n" +
            " 1 | abc | 1 |ab  |\n" +
            " 1 |abcd | 1 | a  |\n" +
            " 1 |abcd | 1 | a  |\n" +
            " 1 |abcd | 1 | a  |\n" +
            " 1 |abcd | 1 | a  |\n" +
            " 1 |abcd | 1 |a b |\n" +
            " 1 |abcd | 1 |ab  |\n" +
            " 1 |abcd | 1 |abc |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "CROSS");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate2, "NESTEDLOOP", "NESTEDLOOP");
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testOuterJoin() throws Exception {
        String sqlTemplate = "select * from t1 a --splice-properties useSpark=" + useSpark.toString() +
                             "\n left outer join t1 b --splice-properties joinStrategy=%s\n on a.b = b.b";
        String sqlTemplate2 = "select * from t11 a --splice-properties useSpark=" + useSpark.toString() +
                             ", joinStrategy=%s\n left outer join t1 b --splice-properties joinStrategy=%s\n on a.b = b.b";
        String sqlText = format(sqlTemplate, "BROADCAST");

        String expected =
            "A |  B  | A |  B  |\n" +
            "--------------------\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 |  a  | 1 |  a  |\n" +
            " 1 | a b | 1 | a b |\n" +
            " 1 | ab  | 1 | ab  |\n" +
            " 1 | abc | 1 | abc |\n" +
            " 1 |abcd | 1 |abcd |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "MERGE");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate2, "NESTEDLOOP", "NESTEDLOOP");
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testAggregationAndPK() throws Exception {
        String sql = "select * from t1 --splice-properties useSpark=" + useSpark.toString() +
                             "\n where b = 'a '";
        String sql2 = "select * from t1 --splice-properties useSpark=" + useSpark.toString() +
                             "\n where b in ('a', 'a ', 'a  ')";
        String sql3 = "select b, count(*) from t1 --splice-properties useSpark=" + useSpark.toString() +
                             "\n group by b";
        String sql4 = "select b from t1 --splice-properties useSpark=" + useSpark.toString() +
                             "\n order by b";
        String sql5 = "select b from t11 --splice-properties useSpark=" + useSpark.toString() +
                             "\n order by b";
        String expected =
            "A | B |\n" +
            "--------\n" +
            " 1 | a |\n" +
            " 1 | a |\n" +
            " 1 | a |\n" +
            " 1 | a |";

        testQuery(sql, expected, methodWatcher);
        testQuery(sql2, expected, methodWatcher);

        expected =
            "B  | 2 |\n" +
            "----------\n" +
            "  a  | 4 |\n" +
            " a b | 1 |\n" +
            " ab  | 1 |\n" +
            " abc | 1 |\n" +
            "abcd | 1 |";

        testQuery(sql3, expected, methodWatcher);

        expected =
            "B  |\n" +
            "------\n" +
            "  a  |\n" +
            "  a  |\n" +
            "  a  |\n" +
            "  a  |\n" +
            " a b |\n" +
            " ab  |\n" +
            " abc |\n" +
            "abcd |";
        
        testQuery(sql4, expected, methodWatcher);
        testQuery(sql5, expected, methodWatcher);

    }

}
