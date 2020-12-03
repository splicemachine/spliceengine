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
import java.util.List;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_UNSUPPORTED_FROM_TABLE_QUERY;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Test the FROM FINAL TABLE clause.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class FromFinalTableIT extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = FromFinalTableIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected int runningOperations;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public FromFinalTableIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {
        String sqlText = "drop trigger trig1";
        try {
            spliceClassWatcher.executeUpdate(sqlText);
        }
        catch (Exception e) {
        }
        sqlText = "drop alias a1";
        try {
            spliceClassWatcher.executeUpdate(sqlText);
        }
        catch (Exception e) {
        }
        sqlText = "drop alias a11";
        try {
            spliceClassWatcher.executeUpdate(sqlText);
        }
        catch (Exception e) {
        }

        sqlText = "drop table if exists t1";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "drop table if exists t11";
		spliceClassWatcher.executeUpdate(sqlText);

        new TableCreator(conn)
                .withCreate("create table t1 (a int, b varchar(10))")
                .withInsert("insert into t1 values(?, ?)")
                .withRows(rows(
                        row(1, "a"),
                        row(2, "a "),
                        row(3, "ab"),
                        row(4, "abc"),
                        row(5, "a b"),
                        row(6, "abcd")))
                .create();

        new TableCreator(conn)
                .withCreate("create table t11 (a int, b varchar(10))")
                .withInsert("insert into t1 values(?, ?)")
                .withRows(rows(
                        row(11, "z"),
                        row(12, "xyz")))
                .create();

        sqlText = "insert into t11 select * from t1";
		spliceClassWatcher.executeUpdate(sqlText);

        new TableCreator(conn)
                .withCreate("create alias a1 for t1")
                .create();
        new TableCreator(conn)
                .withCreate("create synonym a11 for t11")
                .create();
    }

    @Before
    public void createDataSet() throws Exception {
        runningOperations = getNumberOfRunningOperations();
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    public int
    getNumberOfRunningOperations() throws Exception{
        List results = methodWatcher.queryList("CALL SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()");
        return results.size();
    }

    @Test
    public void testVariants() throws Exception {
        methodWatcher.executeUpdate("create trigger trig1\n" +
                            "before update on t1\n" +
                            "referencing new as new\n" +
                            "for each row\n" +
                            "set new.a = -new.a\n");

        String sql = "SELECT b,a FROM OLD TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+1) ";

        String expected =
            "B  | A |\n" +
            "----------\n" +
            "  a  | 1 |\n" +
            "  a  | 2 |\n" +
            " a b | 5 |\n" +
            " ab  | 3 |\n" +
            " abc | 4 |\n" +
            "abcd | 6 |\n" +
            " xyz |12 |\n" +
            "  z  |11 |";

        testQuery(sql, expected, methodWatcher);

        sql = "SELECT b,a+a FROM NEW TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+1) ";

        expected =
            "B  | 2 |\n" +
            "----------\n" +
            "  a  | 2 |\n" +
            "  a  | 4 |\n" +
            " a b |10 |\n" +
            " ab  | 6 |\n" +
            " abc | 8 |\n" +
            "abcd |12 |\n" +
            " xyz |24 |\n" +
            "  z  |22 |";

        testQuery(sql, expected, methodWatcher);

        methodWatcher.executeUpdate("drop trigger trig1");

        methodWatcher.executeUpdate("create trigger trig1\n" +
                            "after update on t1\n" +
                            "referencing new table as new\n" +
                            "for each statement\n" +
                            "insert into t1 select * from t1\n");

        expected =
            "B  | 2 |\n" +
            "----------\n" +
            "  a  | 4 |\n" +
            "  a  | 6 |\n" +
            " a b |12 |\n" +
            " ab  | 8 |\n" +
            " abc |10 |\n" +
            "abcd |14 |\n" +
            " xyz |26 |\n" +
            "  z  |24 |";

        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM FINAL TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+1) ";

        testFail("42603", sql, methodWatcher);
        methodWatcher.executeUpdate("drop trigger trig1");

        sql = "SELECT a, b FROM FINAL TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+1 where b in (select b from t11)) ";
        expected =
            "A |  B  |\n" +
            "----------\n" +
            "13 |  z  |\n" +
            "13 |  z  |\n" +
            "14 | xyz |\n" +
            "14 | xyz |\n" +
            " 3 |  a  |\n" +
            " 3 |  a  |\n" +
            " 4 |  a  |\n" +
            " 4 |  a  |\n" +
            " 5 | ab  |\n" +
            " 5 | ab  |\n" +
            " 6 | abc |\n" +
            " 6 | abc |\n" +
            " 7 | a b |\n" +
            " 7 | a b |\n" +
            " 8 |abcd |\n" +
            " 8 |abcd |";

        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM FINAL TABLE (DELETE FROM t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n WHERE a > 7 and a not in (select a+a from t11)) ";
        testFail("42602", sql, methodWatcher);

        sql = "SELECT a, b FROM OLD TABLE (DELETE FROM t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n WHERE a > 7 and a not in (select a+a from t11)) ";
        expected =
            "A | B  |\n" +
            "---------\n" +
            "13 | z  |\n" +
            "13 | z  |\n" +
            "14 |xyz |\n" +
            "14 |xyz |";
        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM OLD TABLE (INSERT INTO t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n SELECT * from t11 where a not in (select a from t1))";
        testFail("42602", sql, methodWatcher);

        sql = "SELECT a, b FROM NEW TABLE (INSERT INTO t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n SELECT * from t11 where a not in (select a from t1))";
        expected =
            "A | B  |\n" +
            "---------\n" +
            " 1 | a  |\n" +
            "11 | z  |\n" +
            "12 |xyz |\n" +
            " 2 | a  |";
        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM NEW TABLE (INSERT INTO t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n VALUES (1,'hello'))";

        expected =
            "A |  B   |\n" +
            "-----------\n" +
            " 1 |hello |";
        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM NEW TABLE (INSERT INTO t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n SELECT * from t11) WHERE a = 1";

        expected =
            "A | B |\n" +
            "--------\n" +
            " 1 | a |";
        testQuery(sql, expected, methodWatcher);

        // TODO: Add support for this type of statement.
//        sql = "SELECT a, b FROM NEW TABLE (INSERT INTO t1 --splice-properties useSpark=" + useSpark.toString() +
//        "\n SELECT * from t11) WHERE a not in (select a+a from t11)";
//
//        testQuery(sql, expected, methodWatcher);
    }

    @Test
    public void testSynonym() throws Exception {
        String sql = "SELECT a, b FROM FINAL TABLE (INSERT INTO A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n values (1,'alias1')) ";
        String
        expected =
            "A |   B   |\n" +
            "------------\n" +
            " 1 |alias1 |";

        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM FINAL TABLE (INSERT INTO A11 --splice-properties useSpark=" + useSpark.toString() +
        "\n values (1,'alias1')) ";
        testQuery(sql, expected, methodWatcher);

        expected =
            "A |   B   |\n" +
            "------------\n" +
            " 1 |   a   |\n" +
            " 1 |alias1 |\n" +
            "11 |   z   |\n" +
            "12 |  xyz  |\n" +
            " 2 |   a   |\n" +
            " 3 |  ab   |\n" +
            " 4 |  abc  |\n" +
            " 5 |  a b  |\n" +
            " 6 | abcd  |";
        sql = "SELECT a, b FROM FINAL TABLE (INSERT INTO A11 --splice-properties useSpark=" + useSpark.toString() +
        "\n SELECT a,b from A1) ";
        testQuery(sql, expected, methodWatcher);

        expected =
            "A |   B   |\n" +
            "------------\n" +
            "10 |  abc  |\n" +
            "11 |  a b  |\n" +
            "12 | abcd  |\n" +
            "17 |   z   |\n" +
            "18 |  xyz  |\n" +
            " 7 |   a   |\n" +
            " 7 |alias1 |\n" +
            " 8 |   a   |\n" +
            " 9 |  ab   |";
        sql = "SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+6 where b in (select b from a11)) ";
        testQuery(sql, expected, methodWatcher);

        sql = "SELECT a, b FROM OLD TABLE (DELETE FROM A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n where b in (select b from a11)) ";
        testQuery(sql, expected, methodWatcher);

    }

    @Test
    public void testTxnDoesNotLeak() throws Exception {
        methodWatcher.setAutoCommit(false);
        String sql = "SELECT a, b FROM FINAL TABLE (INSERT INTO A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n values (1,'alias1')) ";
        String
        expected =
            "A |   B   |\n" +
            "------------\n" +
            " 1 |alias1 |";

        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);

        String errorMsg = "Current number of running operations:  %d \n" +
                           "does not match original number of running operations:  %d";
        int currentRunningOperations = getNumberOfRunningOperations();
        assertEquals(
          format(errorMsg, currentRunningOperations, runningOperations),
           runningOperations, currentRunningOperations);

        sql = "SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a=1) UNION SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n  set a=1)";
        testFail(LANG_UNSUPPORTED_FROM_TABLE_QUERY, sql, methodWatcher);

        sql = "SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a=1) EXCEPT SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n  set a=1)";
        testFail(LANG_UNSUPPORTED_FROM_TABLE_QUERY, sql, methodWatcher);

        sql = "SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a=1) INTERSECT SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n  set a=1)";
        testFail(LANG_UNSUPPORTED_FROM_TABLE_QUERY, sql, methodWatcher);

        expected =
            "A | B |\n" +
            "--------\n" +
            " 1 | a |";
        sql = "SELECT a, b FROM FINAL TABLE (UPDATE A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a=1 where b='a') ";

        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);

        currentRunningOperations = getNumberOfRunningOperations();
        assertEquals(
          format(errorMsg, currentRunningOperations, runningOperations),
           runningOperations, currentRunningOperations);

        expected = "";
        sql = "SELECT a, b FROM OLD TABLE (DELETE FROM A1 --splice-properties useSpark=" + useSpark.toString() +
        "\n where a=0 \n) ";
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);
        testQuery(sql, expected, methodWatcher);

        currentRunningOperations = getNumberOfRunningOperations();
        assertEquals(
          format(errorMsg, currentRunningOperations, runningOperations),
           runningOperations, currentRunningOperations);

        methodWatcher.rollback();
        methodWatcher.commit();
        methodWatcher.setAutoCommit(true);
    }

    private void loadParamsAndRun(PreparedStatement ps,
                                  Integer paramOneValue,
                                  String paramTwoValue,
                                  String paramThreeValue,
                                  Integer paramFourValue,
                                  String sqlText,
                                  String expected) throws Exception {
            int i = 1;
            if (paramOneValue != null)
                ps.setInt(i++, paramOneValue);
            if (paramTwoValue != null)
                ps.setString(i++, paramTwoValue);
            if (paramThreeValue != null)
                ps.setString(i++, paramThreeValue);
            if (paramFourValue != null)
                ps.setInt(i++, paramFourValue);
            try (ResultSet rs = ps.executeQuery()) {
                assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
    }
    private void testPreparedQuery(String sqlTemplate,
                                   String expected,
                                   Integer paramOneValue,
                                   String paramTwoValue,
                                   String paramThreeValue,
                                   Integer paramFourValue) throws Exception  {
        String sqlText = sqlTemplate;
        try (PreparedStatement ps =
                 methodWatcher.prepareStatement(sqlText)) {
            loadParamsAndRun(ps, paramOneValue, paramTwoValue, paramThreeValue, paramFourValue, sqlText, expected);
        }
    }



    @Test
    public void testParameterizedFromTableQueries() throws Exception {
        methodWatcher.setAutoCommit(false);

        String expected =
            "B  | A |\n" +
            "----------\n" +
            "  a  | 1 |\n" +
            "  a  | 2 |\n" +
            " a b | 5 |\n" +
            " ab  | 3 |\n" +
            " abc | 4 |\n" +
            "abcd | 6 |";

        String sqlTemplate = "SELECT b,a FROM OLD TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+? where b >= ?) WHERE b < ? and a < ?";

        testPreparedQuery(sqlTemplate,  expected, 2, " a", "d", 99);


        sqlTemplate = "SELECT b,a FROM NEW TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = a+? where b >= ?) WHERE b < ? and a < ?";

        expected =
            "B  | A |\n" +
            "----------\n" +
            " ab  |17 |\n" +
            " abc |18 |\n" +
            "abcd |20 |";

        testPreparedQuery(sqlTemplate,  expected, 12, "ab", "d", 22);

        sqlTemplate = "SELECT a, b FROM OLD TABLE (DELETE FROM t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n WHERE a > ? and ? in (select a from t1 b where t1.a = b.a)) ";

        expected =
            "A | B  |\n" +
            "---------\n" +
            "18 |abc |";

        testPreparedQuery(sqlTemplate,  expected, 17, null, null, 18);

        sqlTemplate = "SELECT a, b FROM FINAL TABLE (insert into t1 --splice-properties useSpark=" + useSpark.toString()  +
                      "\n select * from t11 where a != ? and b >= ?)" +
        " WHERE b != ? and a < ? ";

        expected =
            "A |  B  |\n" +
            "----------\n" +
            " 1 |  a  |\n" +
            "11 |  z  |\n" +
            "12 | xyz |\n" +
            " 2 |  a  |\n" +
            " 3 | ab  |\n" +
            " 4 | abc |\n" +
            " 5 | a b |\n" +
            " 6 |abcd |";

        testPreparedQuery(sqlTemplate,  expected, 0, " ", "def", 99);

        sqlTemplate = "SELECT b,a FROM NEW TABLE (UPDATE t1 --splice-properties useSpark=" + useSpark.toString() +
        "\n set a = ?, b = ? WHERE b < ? and a < ?)";

        expected =
            "B     | A |\n" +
            "----------------\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |\n" +
            "Alles klar | 0 |";

        testPreparedQuery(sqlTemplate,  expected, 0, "Alles klar", "x", 99);

        methodWatcher.rollback();
        methodWatcher.commit();
        methodWatcher.setAutoCommit(true);
    }

}
