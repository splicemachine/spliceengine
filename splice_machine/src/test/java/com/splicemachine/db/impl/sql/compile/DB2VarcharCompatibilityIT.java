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
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.derby.utils.SpliceAdmin.INVALIDATE_GLOBAL_DICTIONARY_CACHE;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

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

        new TableCreator(conn)
                .withCreate("create table t (v varchar(10), c char(2))")
                .withIndex("create index ti on t (v,c)")
                .withIndex("create index ti2 on t (c,v)")
                .withInsert("insert into t values(?, ?)")
                .withRows(rows(
                        row("SBVGCCC", "  "),
                        row("SBVGCCC ", "B "),
                        row("SBVGCCC C", "  "),
                        row("SBVGCCC", "A ")))
                .create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
        spliceClassWatcher.executeUpdate("call syscs_util.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', true)");
    }

    @AfterClass
    public static void exitDB2CompatibilityMode() throws Exception {
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', null)");
        spliceClassWatcher.executeUpdate("call syscs_util.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
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
        sqlText = format(sqlTemplate, "SORTMERGE");
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
        sqlText = format(sqlTemplate, "SORTMERGE");
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

    @Test
    public void testInvalidateStoredStatements() throws Exception {
        // Turn off DB2 varchar compatibility mode.
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', null)");
        methodWatcher.executeUpdate("delete from t11");
        methodWatcher.executeUpdate("create trigger trig1\n" +
                                    "after insert on t1\n" +
                                    "referencing new_table as NT\n" +
                                    "for each statement\n" +
                                    "insert into t11\n" +
                                    "select * from t1 where b > 'a'\n");
        // Cause the trigger to be compiled.
        methodWatcher.executeUpdate("insert into t1 values (1, 'a     ')");

        // Turn on DB2 varchar compatibility mode.
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', true)");

        // The following statement should cause all triggers to be recompiled
        // the next time they are fired.
        methodWatcher.executeUpdate("CALL SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");

        methodWatcher.executeUpdate("delete from t11");

        // This statement fires the trigger.
        methodWatcher.executeUpdate("insert into t1 values (1, 'a       ')");

        // t11 should note contain rows with column b values having 'a' followed by spaces
        // since the trigger should have been recompiled with DB2 varchar compatibility mode on,
        // in which case 'a ' > 'a' (from the predicate in the INSERT SELECT in the trigger)
        // should evaluate to false for those rows.

        String expected =
            "A |  B  |\n" +
            "----------\n" +
            " 1 | a b |\n" +
            " 1 | ab  |\n" +
            " 1 | abc |\n" +
            " 1 |abcd |";

        String sqlText = "select * from t11";
        testQuery(sqlText, expected, methodWatcher);
        methodWatcher.executeUpdate("drop trigger trig1");
        methodWatcher.executeUpdate("delete from t11");
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', null)");

        // Delete the rows we created so we don't impact other tests.
        methodWatcher.executeUpdate("delete from t1 where b = 'a     '");
        methodWatcher.executeUpdate("delete from t1 where b = 'a       '");
		methodWatcher.executeUpdate("insert into t11 select * from t1");

        // Restore the flag setting to the value when this test started.
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.varchar.compatible', true)");
    }

    @Test
    public void testMultiProbeScan() throws Exception {
        String sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                            ", index=%s\n" +
                            "where v in ('SBVGCCC    ', 'G')\n" +
                            "and c in ( ' ', CAST('A' AS CHAR(2)))";

        String expected =
            "V    | C |\n" +
            "-------------\n" +
            "SBVGCCC |   |\n" +
            "SBVGCCC | A |";

        String sqlText = format(sqlTemplate, "ti");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "ti2");
        testQuery(sqlText, expected, methodWatcher);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where v = 'SBVGCCC    ' \n" +
                        "and c in ( ' ', CAST('A' AS CHAR(2)))";

        sqlText = format(sqlTemplate, "ti");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "ti2");
        testQuery(sqlText, expected, methodWatcher);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where v in ('SBVGCCC    ', 'G')\n" +
                        "and c = CAST('A' AS CHAR(3))";

        expected =
            "V    | C |\n" +
            "-------------\n" +
            "SBVGCCC | A |";

        sqlText = format(sqlTemplate, "ti");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "ti2");
        testQuery(sqlText, expected, methodWatcher);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where c in ( ' ', CAST('A' AS CHAR(3)))";

        expected =
            "V     | C |\n" +
            "---------------\n" +
            " SBVGCCC  |   |\n" +
            " SBVGCCC  | A |\n" +
            "SBVGCCC C |   |";

        sqlText = format(sqlTemplate, "ti");
        testQuery(sqlText, expected, methodWatcher);
        sqlText = format(sqlTemplate, "ti2");
        testQuery(sqlText, expected, methodWatcher);
    }

    private void loadParamsAndRun(PreparedStatement ps,
                                  boolean skipParamTwo,
                                  boolean skipParamThree,
                                  String sqlText,
                                  String expected) throws Exception {
            int i = 1;
            ps.setString(i++, "SBVGCCC    ");
            if (!skipParamTwo)
                ps.setString(i++, "SBVGCCC ");
            if (!skipParamThree)
                ps.setString(i++, " ");
            ps.setString(i++, "A");
            try (ResultSet rs = ps.executeQuery()) {
                assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
    }
    private void testPreparedQuery1(String sqlTemplate,
                                    String expected,
                                    boolean skipParamTwo,
                                    boolean skipParamThree) throws Exception  {
        String sqlText = format(sqlTemplate, "ti");
        try (PreparedStatement ps =
                 methodWatcher.prepareStatement(sqlText)) {
            loadParamsAndRun(ps, skipParamTwo, skipParamThree, sqlText, expected);
        }

        sqlText = format(sqlTemplate, "ti2");
        try (PreparedStatement ps =
                 methodWatcher.prepareStatement(sqlText)) {
            loadParamsAndRun(ps, skipParamTwo, skipParamThree, sqlText, expected);
        }
    }

    @Test
    public void testParameterizedMultiProbeScan() throws Exception {
        String sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                                ", index=%s\n" +
                                "where v in (?, ?)\n" +
                                "and c in (?, CAST(? AS CHAR(2)))";

        String expected =
            "V    | C |\n" +
            "-------------\n" +
            "SBVGCCC |   |\n" +
            "SBVGCCC | A |";

        testPreparedQuery1(sqlTemplate, expected, false, false);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where v in (?, ?)\n" +
                        "and c in (?, CAST(? AS CHAR(3)))";

        testPreparedQuery1(sqlTemplate, expected, false, false);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where v = ? \n" +
                        "and c in (?, CAST(? AS CHAR(2)))";

        testPreparedQuery1(sqlTemplate, expected, true, false);

        sqlTemplate = "select * from t a --splice-properties useSpark=" + useSpark.toString() +
                        ", index=%s\n" +
                        "where v in (?, ?)\n" +
                        "and c = CAST(? AS CHAR(3))";

        expected =
            "V    | C |\n" +
            "-------------\n" +
            "SBVGCCC | A |";

        testPreparedQuery1(sqlTemplate, expected, false, true);

    }
}
