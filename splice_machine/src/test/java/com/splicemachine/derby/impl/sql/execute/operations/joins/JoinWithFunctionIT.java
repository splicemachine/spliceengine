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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by jyuan on 5/18/16.
 */
@RunWith(Parameterized.class)
public class JoinWithFunctionIT extends SpliceUnitTest {
    private static final String SCHEMA = JoinWithFunctionIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{"NESTEDLOOP"});
        params.add(new Object[]{"SORTMERGE"});
        params.add(new Object[]{"BROADCAST"});
        return params;
    }
    private String joinStrategy;

    public JoinWithFunctionIT(String joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table a (i int, j int)")
                .withInsert("insert into a values(?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(-3, 3), row(-4, 4))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table b (i int, j int)")
                .withInsert("insert into b values(?,?)")
                .withRows(rows(row(-1, 1), row(-2, 2), row(3, 3), row(4, 4))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table c (d double, j int)")
                .withInsert("insert into c values(?,?)")
                .withRows(rows(row(1.1, 1), row(2.2, 2), row(-3.3, 3), row(-4.4, 4))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table d (i int)")
                .withInsert("insert into d values(?)")
                .withRows(rows(row(1), row(2), row(3), row(4))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table e (c char(10))")
                .withInsert("insert into e values(?)")
                .withRows(rows(row("1"), row("2"), row("3"), row("4"))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("CREATE TABLE TABLE_A (ID BIGINT,TYPE VARCHAR(325))")
                .withInsert("insert into table_a values(?,?)")
                .withRows(rows(row(1,"A"))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("CREATE TABLE TABLE_B(ID BIGINT,BD SMALLINT)")
                .withInsert("insert into table_b values(?,?)")
                .withRows(rows(row(1,1))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("CREATE TABLE TABLE_C (ACCOUNT VARCHAR(75),CATEGORY VARCHAR(75),SUB_CATEGORY VARCHAR(75),SOURCE VARCHAR(75),TYPE VARCHAR(500))")
                .withInsert("insert into table_c values(?,?,?,?,?)")
                .withRows(rows(row("ACCOUNT", "CATEGORY", "SUB_CATEGORY", "PBD_SMARTWORKS", "A"))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("CREATE TABLE CFTC1(col1 DATE)")
                .withInsert("insert into CFTC1 values (?)")
                .withRows(rows(row("2018-08-02")))
                .create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("CREATE TABLE CFTC2(col2 char(8))")
                .withInsert("insert into CFTC2 values (?)")
                .withRows(rows(row("02.08.18")))
                .create();
    }

    @Test
    public void testUnaryFunction() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "a\n" +
                ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                "where abs(a.i)=abs(b.i) order by a.i", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected =
                "I | J | I | J |\n" +
                "----------------\n" +
                "-4 | 4 | 4 | 4 |\n" +
                "-3 | 3 | 3 | 3 |\n" +
                " 1 | 1 |-1 | 1 |\n" +
                " 2 | 2 |-2 | 2 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testJavaFunction() throws Exception {
        String sql = String.format("select a.i, a.j, c.d from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "a\n" +
                ", c --SPLICE-PROPERTIES joinStrategy=%s\n" +
                " where double(a.i)=floor(c.d) order by a.i", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);

        String expected =
                "I | J |  D  |\n" +
                "--------------\n" +
                "-4 | 4 |-3.3 |\n" +
                " 1 | 1 | 1.1 |\n" +
                " 2 | 2 | 2.2 |";

        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(s, expected, s);
        rs.close();
    }

    @Test
    public void testBinaryFunction() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "a\n" +
                ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                "where mod(abs(a.i), 10) = abs(b.i) order by a.i", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);

        String expected =
                "I | J | I | J |\n" +
                "----------------\n" +
                "-4 | 4 | 4 | 4 |\n" +
                "-3 | 3 | 3 | 3 |\n" +
                " 1 | 1 |-1 | 1 |\n" +
                " 2 | 2 |-2 | 2 |";

        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(s, expected, s);
        rs.close();
    }

    @Test
    public void testBinaryArithmeticOperator() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "a\n" +
                ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                ", c --SPLICE-PROPERTIES joinStrategy=%s\n" +
                "where a.j+1 = b.j+1 and b.j+1 = c.j+1 order by a.i", joinStrategy, joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "I | J | I | J |  D  | J |\n" +
                "--------------------------\n" +
                "-4 | 4 | 4 | 4 |-4.4 | 4 |\n" +
                "-3 | 3 | 3 | 3 |-3.3 | 3 |\n" +
                " 1 | 1 |-1 | 1 | 1.1 | 1 |\n" +
                " 2 | 2 |-2 | 2 | 2.2 | 2 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testSelfJoinWithBinaryArithmeticOperator() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                " a t1 --SPLICE-PROPERTIES joinStrategy=%s \n" +
                " , a t2 where t1.i*2=t2.j order by t1.i", joinStrategy);

        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "I | J | I | J |\n" +
                "----------------\n" +
                " 1 | 1 | 2 | 2 |\n" +
                " 2 | 2 |-4 | 4 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testCharFunction()  throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "d\n" +
                ", e --SPLICE-PROPERTIES joinStrategy=%s\n" +
                " where CHAR(d.i) = e.c order by e.c", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "I | C |\n" +
                        "--------\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |";
        assertEquals(s, expected, s);
    }


    @Test
    public void testLeftOuterJoin() throws Exception {
        String sqlText = "SELECT C.ACCOUNT ,C.CATEGORY, C.SUB_CATEGORY\n" +
                "FROM TABLE_A A\n" +
                "LEFT JOIN TABLE_B B\n" +
                "ON A.ID = B.ID\n" +
                "LEFT JOIN TABLE_C C\n" +
                "ON C.SOURCE= 'PBD_SMARTWORKS'\n" +
                "and C.TYPE=UPPER(A.type)\n" +
                "WHERE B.BD = 1";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "ACCOUNT |CATEGORY |SUB_CATEGORY |\n" +
                "----------------------------------\n" +
                " ACCOUNT |CATEGORY |SUB_CATEGORY |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testJoinExpressionWithMultipleColumnReferences() throws Exception {
        // expression with multiple column references
        try {
            String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                    "a\n" +
                    ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                    "where a.j-a.i = b.j+b.i order by a.i, a.j, b.i, b.j", joinStrategy, joinStrategy);
            ResultSet rs = methodWatcher.executeQuery(sql);
            String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            rs.close();
            String expected =
                    "I | J | I | J |\n" +
                            "----------------\n" +
                            "-4 | 4 | 4 | 4 |\n" +
                            "-3 | 3 | 3 | 3 |\n" +
                            " 1 | 1 |-2 | 2 |\n" +
                            " 1 | 1 |-1 | 1 |\n" +
                            " 2 | 2 |-2 | 2 |\n" +
                            " 2 | 2 |-1 | 1 |";
            assertEquals(s, expected, s);
        } catch (SQLException se) {
            Assert.assertTrue(!this.joinStrategy.equals("NESTEDLOOP") && se.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }

        // expression with a concatenation of multiple binary operations
        try {
            String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                    "a\n" +
                    ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                    "where a.j-a.i+a.j = b.j+b.i+b.j order by a.i, a.j, b.i, b.j", joinStrategy, joinStrategy);
            ResultSet rs = methodWatcher.executeQuery(sql);
            String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            rs.close();
            String expected =
                    "I | J | I | J |\n" +
                            "----------------\n" +
                            "-4 | 4 | 4 | 4 |\n" +
                            "-3 | 3 | 3 | 3 |\n" +
                            " 1 | 1 |-1 | 1 |\n" +
                            " 2 | 2 |-2 | 2 |";
            assertEquals(s, expected, s);
        } catch (SQLException se) {
            Assert.assertTrue(!this.joinStrategy.equals("NESTEDLOOP") && se.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void testJoinExpressionWithSQLFunctionWithOneColumnReference() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "CFTC1\n" +
                ", CFTC2  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                "where TO_CHAR(col1,'dd.mm.yy') = col2", joinStrategy, joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "COL1    |  COL2   |\n" +
                        "----------------------\n" +
                        "2018-08-02 |02.08.18 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testJoinExpressionOverSetOperation() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "d\n" +
                ", (select substr(c, 1, 5) as newC from e " +
                "   intersect " +
                "   select substr(c, 1, 5) as newC from e) dt --SPLICE-PROPERTIES joinStrategy=%s\n" +
                " where substr(CHAR(d.i),1,1) = substr(dt.newC, 1, 1) order by 1", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "I |NEWC |\n" +
                        "----------\n" +
                        " 1 |  1  |\n" +
                        " 2 |  2  |\n" +
                        " 3 |  3  |\n" +
                        " 4 |  4  |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testJoinExpressionOverDTWithSetOperation() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "d\n" +
                ", (select newC from (select substr(c, 1, 5) as newC from e " +
                "                     intersect " +
                "                     select substr(c, 1, 5) as newC from e) dt0) dt  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                " where substr(CHAR(d.i),1,1) = substr(dt.newC, 1, 1) order by 1", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();
        String expected =
                "I |NEWC |\n" +
                        "----------\n" +
                        " 1 |  1  |\n" +
                        " 2 |  2  |\n" +
                        " 3 |  3  |\n" +
                        " 4 |  4  |";
        assertEquals(s, expected, s);
    }

}
