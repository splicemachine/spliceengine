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

package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.SchemaDAO;
import org.junit.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.homeless.TestUtils.o;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test table subqueries -- subqueries that can return multiple rows and columns.  These can appear only in FROM,
 * IN, ALL, ANY, or EXISTS parts of the enclosing query.
 */
public class Subquery_Table_IT extends SpliceUnitTest {

    private static final String SCHEMA = Subquery_Table_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createdSharedTables() throws Exception {
        TestConnection conn = classWatcher.getOrCreateConnection();
        try(Statement s = conn.createStatement()){
            SchemaDAO schemaDAO = new SchemaDAO(conn);
            schemaDAO.drop(SCHEMA);

            s.executeUpdate("create schema "+SCHEMA);
            conn.setSchema(SCHEMA.toUpperCase());
            s.executeUpdate("create table T1 (k int, l int)");
            s.executeUpdate("insert into T1 values (0,1),(0,1),(1,2),(2,3),(2,3),(3,4),(4,5),(4,5),(5,6),(6,7),(6,7),(7,8),(8,9),(8,9),(9,10)");

            s.executeUpdate("create table T2 (k int, l int)");
            s.executeUpdate("insert into T2 values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10)");

            s.executeUpdate("create table T3 (i int)");
            s.executeUpdate("insert into T3 values (10),(20)");

            s.executeUpdate("create table T4 (i int)");
            s.executeUpdate("insert into T4 values (30),(40)");

            TestUtils.executeSqlFile(conn, "test_data/employee.sql", SCHEMA);
            TestUtils.executeSqlFile(conn, "null_int_data.sql", SCHEMA);
            TestUtils.executeSqlFile(conn, "test_data/content.sql", SCHEMA);

            s.executeUpdate("create table s (a int, b int, c int, d int, e int, f int)");
            s.executeUpdate("insert into s values (0,1,2,3,4,5)");
            s.executeUpdate("insert into s values (10,11,12,13,14,15)");
            s.executeUpdate("create table parentT ( i int, j int, k int)");
            s.executeUpdate("create table childT ( i int, j int, k int)");
            s.executeUpdate("insert into parentT values (1,1,1), (2,2,2), (3,3,3), (4,4,4)");
            s.executeUpdate("insert into parentT select i+4, j+4, k+4 from parentT");
            s.executeUpdate("insert into parentT select i+8, j+8, k+8 from parentT");
            s.executeUpdate("insert into parentT select i+16, j+16, k+16 from parentT");
            s.executeUpdate("insert into parentT select i+32, j+32, k+32 from parentT");
            s.executeUpdate("insert into parentT select i+64, j+64, k+64 from parentT");
            s.executeUpdate("insert into parentT select i+128, j+128, k+128 from parentT");
            s.executeUpdate("insert into parentT select i+256, j+256, k+256 from parentT");
            s.executeUpdate("insert into parentT select i+512, j+512, k+512 from parentT");
            s.executeUpdate("insert into parentT select i+1024, j+1024, k+1024 from parentT");
            //s.executeUpdate("insert into childT select * from parentT");

            s.executeUpdate("insert into childT values (1,1,1), (2,2,2), (3,3,3), (4,4,4)");
            s.executeUpdate("insert into childT select i+4, j+4, k+4 from parentT");
            s.executeUpdate("insert into childT select i+8, j+8, k+8 from parentT");
            s.executeUpdate("insert into childT select i+16, j+16, k+16 from parentT");
            s.executeUpdate("insert into childT select i+32, j+32, k+32 from parentT");
            s.executeUpdate("insert into childT select i+64, j+64, k+64 from parentT");
            s.executeUpdate("insert into childT select i+128, j+128, k+128 from parentT");
            s.executeUpdate("insert into childT select i+256, j+256, k+256 from parentT");
            s.executeUpdate("insert into childT select i+512, j+512, k+512 from parentT");
            s.executeUpdate("insert into childT select i+1024, j+1024, k+1024 from parentT");

            s.executeUpdate("create table tt1 (a1 int, b1 int, c1 int)");
            s.executeUpdate("insert into tt1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4)");
            s.executeUpdate("create table tt2 (a2 int, b2 int, c2 int)");
            s.executeUpdate("insert into tt2 select * from tt1");
            s.executeUpdate("create table tt3 (a3 int, b3 int, c3 int)");
            s.executeUpdate("insert into tt3 select * from tt1");
            s.executeUpdate("insert into tt3 select * from tt1");
            s.executeUpdate("create table tt4 (a4 int, b4 int, c4 int)");
            s.executeUpdate("insert into tt4 select * from tt1");
            s.executeUpdate("insert into tt4 select * from tt1");

            s.executeUpdate("create table tab1 (a1 int not null, b1 int)");
            s.executeUpdate("insert into tab1 values (1,1),(2,2),(3,3)");
            s.executeUpdate("create table tab2 (a2 int not null, b2 int)");
            s.executeUpdate("insert into tab2 values (1,1),(1,1),(2,2), (2,2)");
            s.executeUpdate("create table tab3 (a3 int not null, b3 int)");
            s.executeUpdate("insert into tab3 values (1,1),(2,2),(4,4)");
            s.executeUpdate("create table tab4 (a4 int not null, b4 int)");
            s.executeUpdate("insert into tab4 values (1,1),(2,2),(5,5)");
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // IN ( <subquery> )
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* Regression test for bug 273 */
    @Test
    public void subqueryInJoinClause() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from T1 a join T2 b on a.k = b.k and a.k in (select k from T1 where a.k =T1.k)");
        assertUnorderedResult(rs, "" +
                "K | L | K | L |\n" +
                "----------------\n" +
                " 0 | 1 | 0 | 1 |\n" +
                " 0 | 1 | 0 | 1 |\n" +
                " 1 | 2 | 1 | 2 |\n" +
                " 2 | 3 | 2 | 3 |\n" +
                " 2 | 3 | 2 | 3 |\n" +
                " 3 | 4 | 3 | 4 |\n" +
                " 4 | 5 | 4 | 5 |\n" +
                " 4 | 5 | 4 | 5 |\n" +
                " 5 | 6 | 5 | 6 |\n" +
                " 6 | 7 | 6 | 7 |\n" +
                " 6 | 7 | 6 | 7 |\n" +
                " 7 | 8 | 7 | 8 |\n" +
                " 8 | 9 | 8 | 9 |\n" +
                " 8 | 9 | 8 | 9 |\n" +
                " 9 |10 | 9 |10 |");
    }

    @Test
    public void inDoesNotReturnDuplicates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select k from t2 a where k in (select k from T1 )");
        assertUnorderedResult(rs, "" +
                "K |\n" +
                "----\n" +
                " 0 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 6 |\n" +
                " 7 |\n" +
                " 8 |\n" +
                " 9 |");
    }

    /* Regression test for DB-954 */
    @Test
    public void inWeirdQuery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("" +
                "select i from t3 a where i in " +
                "(select a.i from t4 where a.i < i union all select i from t4 where 1 < 0)");
        assertUnorderedResult(rs, "" +
                "I |\n" +
                "----\n" +
                "10 |\n" +
                "20 |");
    }

    // JIRA 1121
    @Test(timeout = 30000)
    public void inSubqueryTooBigToMaterialize() throws Exception {
        String sql = "select count(*) from parentT where i < 10 and i not in (select i from childT)";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertUnorderedResult(rs, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // DB-4494: re-run the query using a different connection
        Connection connection = methodWatcher.createConnection();
        Statement s = connection.createStatement();
        rs = s.executeQuery(sql);
        assertUnorderedResult(rs, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");
    }

    @Test(timeout = 30000)
    public void inSubqueryTooBigToMaterializeSpark() throws Exception {
        String sql = "select count(*) from parentT --splice-properties useSpark=true \n where i < 10 and i not in (select i from childT)";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertUnorderedResult(rs, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // DB-4494: re-run the query using a different connection
        Connection connection = methodWatcher.createConnection();
        Statement s = connection.createStatement();
        rs = s.executeQuery(sql);
        assertUnorderedResult(rs, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // EXISTS <subquery>
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void existsCorrelatedDoubleNestedNotExists() throws Exception {
        List<Object[]> expected = Collections.singletonList(o("Alice"));

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT STAFF.EMPNAME" +
                        "          FROM STAFF" +
                        "          WHERE NOT EXISTS" +
                        "                 (SELECT *" +
                        "                       FROM PROJ" +
                        "                       WHERE NOT EXISTS" +
                        "                             (SELECT *" +
                        "                                   FROM WORKS" +
                        "                                   WHERE STAFF.EMPNUM = WORKS.EMPNUM" +
                        "                                   AND WORKS.PNUM=PROJ.PNUM))");

        assertArrayEquals(expected.toArray(), TestUtils.resultSetToArrays(rs).toArray());
    }

    @Test
    public void existsAggWithDoublyNestedCorrelatedSubquery() throws Exception {
        // As of work in progress for 2547, this one IT fails (actually it never comes back)
        // when splice.temp.bucketCount is set to 32. All other enabled ITs pass.
        List<Object[]> expected = Arrays.asList(o("P1", BigDecimal.valueOf(80)), o("P5", BigDecimal.valueOf(92)));

        ResultSet rs = methodWatcher.executeQuery("SELECT pnum, " +
                "       Sum(hours) " +
                "FROM   works c " +
                "GROUP  BY pnum " +
                "HAVING EXISTS (SELECT pname " +
                "               FROM   proj, " +
                "                      works a " +
                "               WHERE  proj.pnum = a.pnum " +
                "                      AND proj.budget / 200 < (SELECT Sum(hours) " +
                "                                               FROM   works b " +
                "                                               WHERE  a.pnum = b.pnum " +
                "                                                      AND a.pnum = c.pnum))" +
                "ORDER BY pnum");

        assertArrayEquals(expected.toArray(), TestUtils.resultSetToArrays(rs).toArray());
    }

    @Test
    public void existsSubqueryWithLiteralProjection() throws Exception {
        List<Object[]> expected = Arrays.asList(o("E3"), o("E5"));

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT EMPNUM FROM STAFF O " +
                        "WHERE EXISTS (" +
                        // make subquery a union so is not converted to join
                        "   SELECT 1 FROM STAFF WHERE 1 = 0 " +
                        "   UNION " +
                        "   SELECT 1 FROM STAFF I " +
                        "   WHERE O.EMPNUM = I.EMPNUM AND I.GRADE > 12) " +
                        "ORDER BY EMPNUM");

        assertArrayEquals(expected.toArray(), TestUtils.resultSetToArrays(rs).toArray());
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // ANY <subquery>
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void anySubquery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from z1 where z1.s >= ANY (select z2.b from z2)");
        assertUnorderedResult(rs, "" +
                "I | S | C |VC | B |\n" +
                "--------------------\n" +
                " 0 | 0 | 0 | 0 | 0 |\n" +
                " 1 | 1 | 1 | 1 | 1 |");

        rs = methodWatcher.executeQuery("select * from z1 where z1.s < ANY (select z2.b from z2)");
        assertUnorderedResult(rs, "" +
                "I | S | C |VC | B |\n" +
                "--------------------\n" +
                " 0 | 0 | 0 | 0 | 0 |");

        rs = methodWatcher.executeQuery("select * from z1 where z1.s > ANY (select z2.b from z2)");
        assertUnorderedResult(rs, "" +
                "I | S | C |VC | B |\n" +
                "--------------------\n" +
                " 1 | 1 | 1 | 1 | 1 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // FROM <subquery>
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void joinOfTwoSubqueries() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("" +
                "select * from (select a.a, b.a from s a, s b) a (b, a), " +
                "(select a.a, b.a from s a, s b) b (b, a) where a.b = b.b");
        assertUnorderedResult(rs, "" +
                "B | A | B | A |\n" +
                "----------------\n" +
                " 0 | 0 | 0 | 0 |\n" +
                " 0 | 0 | 0 |10 |\n" +
                " 0 |10 | 0 | 0 |\n" +
                " 0 |10 | 0 |10 |\n" +
                "10 | 0 |10 | 0 |\n" +
                "10 | 0 |10 |10 |\n" +
                "10 |10 |10 | 0 |\n" +
                "10 |10 |10 |10 |");
    }

    /* Regression test for DB-1027 */
    @Test
    public void countOverSubqueryWithJoin() throws Exception {
        methodWatcher.executeUpdate("create table cols (ID VARCHAR(128) NOT NULL, COLLID SMALLINT NOT NULL)");
        methodWatcher.executeUpdate("insert into cols values ('123', 2), ('124', -5), ('24', 1), ('26', -2), ('36', 1), ('37', 8)");

        methodWatcher.executeUpdate("create table docs (ID VARCHAR(128) NOT NULL)");
        methodWatcher.executeUpdate("insert into docs values ('24'), ('25'), ('27'), ('36'), ('124'), ('567')");

        ResultSet rs = methodWatcher.executeQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT ID FROM docs WHERE ID NOT IN (SELECT ID FROM cols WHERE COLLID IN (-2,1))) AS TAB");
        assertUnorderedResult(rs, "" +
                "1 |\n" +
                "----\n" +
                " 4 |");
    }

    @Test
    public void joinOfAggSubquery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "SELECT S.DESCRIPTION, FAV.MAXRATE, C.TITLE, C.URL " +
                        "FROM RATING R, " +
                        "      CONTENT C, " +
                        "      STYLE S, " +
                        "      CONTENT_STYLE CS, " +
                        "      (select S.ID, max(rating) " +
                        "         from RATING R, CONTENT C, STYLE S," +
                        "            CONTENT_STYLE CS group by S.ID) AS FAV(FID,MAXRATE) " +
                        "WHERE  R.ID        = C.ID" +
                        "   AND C.ID        = CS.CONTENT_ID " +
                        "   AND CS.STYLE_ID = FAV.FID " +
                        "   AND FAV.FID     = S.ID AND" +
                        "   FAV.MAXRATE     = R.RATING " +
                        "ORDER BY S.DESCRIPTION");

        assertOrderedResult(rs, "" +
                "DESCRIPTION | MAXRATE | TITLE |     URL     |\n" +
                "----------------------------------------------\n" +
                "    BIRD     |   4.5   |title1 |http://url.1 |\n" +
                "     CAR     |   4.5   |title1 |http://url.1 |");    }

    /* Regression test for DB-961 */
    @Test
    public void fromSubqueryOverScalarAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from (select max(i) from T3) q,T4");
        assertUnorderedResult(rs, "" +
                "1 | I |\n" +
                "--------\n" +
                "20 |30 |\n" +
                "20 |40 |");
    }

    @Test
    public void orderByInSubQuery() throws Exception {
        try {
            methodWatcher.executeQuery("select * from (select a, b from s order by d ) as vt order by d ");
            Assert.fail("Error not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("42X04", e.getSQLState());
        }

        try {
            methodWatcher.executeQuery("select * from (select a, b, c from s order by c, d ) as vt order by d ");
            Assert.fail("Error not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("42X04", e.getSQLState());
        }

        ResultSet rs = methodWatcher.executeQuery("select * from (select a, b, d from s order by d ) as vt order by d ");
        assertUnorderedResult(rs, "" +
                "A | B | D |\n" +
                "------------\n" +
                " 0 | 1 | 3 |\n" +
                "10 |11 |13 |");

        ResultSet rs1 = methodWatcher.executeQuery("select a, b, d from (select * from s order by d ) as vt order by d ");
        assertUnorderedResult(rs1, "" +
                "A | B | D |\n" +
                "------------\n" +
                " 0 | 1 | 3 |\n" +
                "10 |11 |13 |");
    }

    @Test
    public void testNonCorrelatedInSubqueryViaControl() throws Exception {
        /* test logic to convert where clause non-flattenable, non-correlated subquery to
           from subquery (DT), so that they can be joined to the outer table through inclusion join
         */
        /* Q1: non-correlated IN subquery with more than one table */
        String sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: non-correlated IN subquery with nested correlated subquery */
        sql = "select * from tt1  --splice-properties useSpark=false\n where a1 in (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q3: non-correlated IN subquery with nested non-correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a3 from tt3 where a3 in (select a4 from tt4 where b4=1))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q4: non-correlated subquery with multiple tables on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q5: non-correlated subquery with expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1+a2 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q6: non-correlated subquery with expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 in (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q7: constant expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and 3 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q8: constant expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 in (select 3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q9: multiple level of non-correlated subqueries */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a2 from tt2 where c2= 3 and b2 in (select 3 from tt3, tt4 where b3=b4))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q10: negative test case, non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q11: negative test case, NOT IN non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 not in (select a2 from tt2, tt4 where b2=b4 and b2=3)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedInSubqueryViaSpark() throws Exception {
        /* test logic to convert where clause non-flattenable, non-correlated subquery to
           from subquery (DT), so that they can be joined to the outer table through inclusion join
         */
        /* Q1: non-correlated IN subquery with more than one table */
        String sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: non-correlated IN subquery with nested correlated subquery */
        sql = "select * from tt1  --splice-properties useSpark=true\n where a1 in (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q3: non-correlated IN subquery with nested non-correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a3 from tt3 where a3 in (select a4 from tt4 where b4=1))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q4: non-correlated subquery with multiple tables on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q5: non-correlated subquery with expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1+a2 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q6: non-correlated subquery with expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 in (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q7: constant expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and 3 in (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q8: constant expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 in (select 3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q9: multiple level of non-correlated subqueries */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a2 from tt2 where c2= 3 and b2 in (select 3 from tt3, tt4 where b3=b4))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q10: negative test case, non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q11: negative test case, NOT IN non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 not in (select a2 from tt2, tt4 where b2=b4 and b2=3)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedExistsSubqueryViaControl() throws Exception {
        /* many exists have been flattened before the optimize phase's preprocess(), for the remaining
           cases that cannot be flattened and it is a non-correlated subquery, we convert it to
           from subquery (DT), so that they can be joined to the outer table through inclusion join
         */
        /* Q1: non-correlated EXISTS subquery with more than one table, exists is flattened */
        String sql = "select * from tt1 --splice-properties useSpark=false\n where exists (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q2: non-correlated EXISTS subquery with nested correlated subquery, exists is not flattened */
        sql = "select * from tt1 --splice-properties useSpark=false\n where exists (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q3: non-correlated EXISTS subquery with nested non-correlated subquery, exists is flattened */
        sql = "select * from tt1 --splice-properties useSpark=false\n where exists (select a3 from tt3 where a3 in (select a4 from tt4 where b4=0))";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: non-correlated subquery with multiple tables on left, exists is flattened */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where a1=a2 and exists (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q5: non-correlated subquery with expression on right, exists is flattened */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and exists (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q6: negative test case, NOT exists */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and not exists (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedExistsSubqueryViaSpark() throws Exception {
        /* many exists have been flattened before the optimize phase's preprocess(), for the remaining
           cases that cannot be flattened and it is a non-correlated subquery, we convert it to
           from subquery (DT), so that they can be joined to the outer table through inclusion join
         */
        /* Q1: non-correlated EXISTS subquery with more than one table, exists is flattened */
        String sql = "select * from tt1 --splice-properties useSpark=true\n where exists (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q2: non-correlated EXISTS subquery with nested correlated subquery, exists is not flattened */
        sql = "select * from tt1 --splice-properties useSpark=true\n where exists (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q3: non-correlated EXISTS subquery with nested non-correlated subquery, exists is flattened */
        sql = "select * from tt1 --splice-properties useSpark=true\n where exists (select a3 from tt3 where a3 in (select a4 from tt4 where b4=0))";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: non-correlated subquery with multiple tables on left, exists is flattened */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where a1=a2 and exists (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q5: non-correlated subquery with expression on right, exists is not flattened */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and exists (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q6: negative test case, NOT exists */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and not exists (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedSubqueryWithANYQualifierViaControl() throws Exception {
        /* Q1: non-correlated subquery with more than one table */
        String sql = "select * from tt1 --splice-properties useSpark=false\n where a1 = ANY (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: non-correlated subquery with nested correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 > ANY (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q3: non-correlated subquery with nested non-correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 <= ANY (select a3 from tt3 where a3 in (select a4 from tt4 where b4=1))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q4: non-correlated subquery with multiple tables on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 <> ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q5: non-correlated subquery with expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1+a2 = ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q6: non-correlated subquery with expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 = ANY (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q7: constant expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and 3 > ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q8: constant expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=false\n where b1=b2 and a1 > ANY (select 3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q9: negative test case, non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 = ANY (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q10: negative test case, ALL quantifier cannot be converted*/
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 > ALL (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedSubqueryWithANYQualifierViaSpark() throws Exception {
        /* Q1: non-correlated subquery with more than one table */
        String sql = "select * from tt1 --splice-properties useSpark=true\n where a1 = ANY (select a2 from tt2, tt4 where b2=b4)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: non-correlated subquery with nested correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 > ANY (select a3 from tt3 where a3 in (select a4 from tt4 where b3=b4 and b4 in (1,2)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q3: non-correlated subquery with nested non-correlated subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 <= ANY (select a3 from tt3 where a3 in (select a4 from tt4 where b4=1))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q4: non-correlated subquery with multiple tables on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 <> ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q5: non-correlated subquery with expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1+a2 = ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q6: non-correlated subquery with expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 = ANY (select a3+a4 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q7: constant expression on left */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and 3 > ANY (select a3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q8: constant expression on right */
        sql = "select * from tt1, tt2 --splice-properties useSpark=true\n where b1=b2 and a1 > ANY (select 3 from tt3, tt4 where b3=b4)";
        expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q9: negative test case, non-flattened correlated subquery are not converted to from subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 = ANY (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 2 | 2 |\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q10: negative test case, ALL quantifier cannot be converted*/
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 > ALL (select a2 from tt2, tt4 where b2=b4 and b1=b2)";
        expected = "";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testNonCorrelatedSubqueryWithAggregationViaControlPath() throws Exception {
        /* Q1: subquery with having clause condition */
        String sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a2 from tt2 group by a2 having a2=3)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: subquery with having subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a2 from tt2 group by a2 having a2 in (values (3)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q3: subquery with having subquery */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select a3 from tt3 group by a3 having a3>3 and count(*) in (select a2 from tt2))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: subquery with aggregation in select clause */
        sql = "select * from tt1 --splice-properties useSpark=false\n where a1 in (select count(*) from tt3 group by a3)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testNonCorrelatedSubqueryWithAggregationViaSparkPath() throws Exception {
        /* Q1: subquery with having clause condition */
        String sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a2 from tt2 group by a2 having a2=3)";
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: subquery with having subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a2 from tt2 group by a2 having a2 in (values (3)))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q3: subquery with having subquery */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select a3 from tt3 group by a3 having a3>3 and count(*) in (select a2 from tt2))";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 4 | 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: subquery with aggregation in select clause */
        sql = "select * from tt1 --splice-properties useSpark=true\n where a1 in (select count(*) from tt3 group by a3)";
        expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 2 | 2 | 2 |";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testSubqueryInOuterJoin() throws Exception {
        String sql = "select 1 from tt1 left join (select a2 from tt2 where a2 not in (select a3 from tt3)) dt on a1=a2";
        String expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        ResultSet rs = conn().createStatement().executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSemiJoinWithExistsSubquery() throws Exception {
        String sqlText = "select * from tab3, tab1 where exists (select 1 from tab2 where a1=a2)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB3", "TAB2", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 1 | 1 |\n" +
                        " 1 | 1 | 2 | 2 |\n" +
                        " 2 | 2 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 |\n" +
                        " 4 | 4 | 1 | 1 |\n" +
                        " 4 | 4 | 2 | 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSemiJoinWithInSubquery() throws Exception {
        String sqlText = "select * from TAB3, TAB1 where b1 in (select b2 from TAB2 where a1=a2)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB3", "TAB2", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 1 | 1 |\n" +
                        " 1 | 1 | 2 | 2 |\n" +
                        " 2 | 2 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 |\n" +
                        " 4 | 4 | 1 | 1 |\n" +
                        " 4 | 4 | 2 | 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSemiJoinWithInSubqueryWithMultipleTables() throws Exception {
        String sqlText = "select * from tab1, tab4 where b1 in (select b2 from tab2, tab3  where a2=a3)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB4", "TAB3", "TAB2", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |B1 |A4 |B4 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 1 | 1 |\n" +
                        " 1 | 1 | 2 | 2 |\n" +
                        " 1 | 1 | 5 | 5 |\n" +
                        " 2 | 2 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 |\n" +
                        " 2 | 2 | 5 | 5 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSemiJoinWithNotExistsSubquery() throws Exception {
        String sqlText = "select * from tab3, tab1 where not exists (select 1 from tab2 where a1=a2)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB3", "TAB2", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 3 | 3 |\n" +
                        " 2 | 2 | 3 | 3 |\n" +
                        " 4 | 4 | 3 | 3 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSemiJoinWithNotInSubquery() throws Exception {
        String sqlText = "select * from tab3, tab1 where a1 not in (select a2 from tab2)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB3", "TAB2", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 3 | 3 |\n" +
                        " 2 | 2 | 3 | 3 |\n" +
                        " 4 | 4 | 3 | 3 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testDoNotFlattenHintForInSubquery() throws Exception {
        String sqlText = "select * from tab3, tab1 where a1 not in (select a2 from tab2) --splice-properties doNotFlatten=true";

        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 3 | 3 |\n" +
                        " 2 | 2 | 3 | 3 |\n" +
                        " 4 | 4 | 3 | 3 |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select * from tab3, tab1 where a1 not in (select a2 from tab2) --splice-properties doNotFlatten=false";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testDoNotFlattenHintForSubqueryWithQualifier() throws Exception {
        String sqlText = "select * from tab1 where a1 = ANY (select a2 from tab2 where b1=b2) --splice-properties doNotFlatten=true";

        String expected =
                "A1 |B1 |\n" +
                        "--------\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select * from tab1 where a1 = ANY (select a2 from tab2 where b1=b2) --splice-properties doNotFlatten=false";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testDoNotFlattenHintForExistsSubquery() throws Exception {
        String sqlText = "select * from tab3, tab1 where not exists (select 1 from tab2 where a1=a2) --splice-properties doNotFlatten=true";

        String expected =
                "A3 |B3 |A1 |B1 |\n" +
                        "----------------\n" +
                        " 1 | 1 | 3 | 3 |\n" +
                        " 2 | 2 | 3 | 3 |\n" +
                        " 4 | 4 | 3 | 3 |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select * from tab3, tab1 where not exists (select 1 from tab2 where a1=a2) --splice-properties doNotFlatten=false";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testDoNotFlattenHintForAggregateSubquery() throws Exception {
        String sqlText = "select * from tt1 where a1 in (select count(*) from tt3 group by a3) --splice-properties doNotFlatten=true";

        String expected =
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 2 | 2 | 2 |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select * from tt1 where a1 in (select count(*) from tt3 group by a3) --splice-properties doNotFlatten=false";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testDoNotFlattenHintForSelectScalarSubquery() throws Exception {
        /* scalar subquery in predicate is not flattened */
        String sqlText = "select (select distinct a2 from tab2 where b1=b2) --splice-properties doNotFlatten=true\n " +
                "as X from tab1";

        String expected =
                "X  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select (select distinct a2 from tab2 where b1=b2) --splice-properties doNotFlatten=false\n " +
                "as X from tab1";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testDoNotFlattenHintForWhereScalarSubquery() throws Exception {
        /* scalar subquery in predicate is not flattened */
        String sqlText = "select * from tab1 where (select max(a2) from tab2 where b1=b2) --splice-properties doNotFlatten=true\n is not null";

        String expected =
                "A1 |B1 |\n" +
                        "--------\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        sqlText = "select * from tab1 where (select max(a2) from tab2 where b1=b2) --splice-properties doNotFlatten=false\n is not null";

        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testSemiJoinWithNonCorrelatedExistsSubquery() throws Exception {
        String sqlText = "select * from tab1, tab3 --splice-properties useDefaultRowCount=300\n" +
                " where not exists (select 1 from tab2)";

        /* With the rewrite rule change from DB-8448, we no longer flatten non-correlated exists/not exists */
        SubqueryITUtil.assertUnorderedResult(conn(), sqlText, SubqueryITUtil.ONE_SUBQUERY_NODE, "");
    }

    @Test
    public void testSemiJoinWithSubqueryCorrelatedToMultipleOuterTables() throws Exception {
        String sqlText = "select * from tab3, tab1, tab4 where exists (select 1 from tab2 where a1=a2 and a2=a4)";

        testOrderOfTables("explain " + sqlText, Arrays.asList("TAB3", "TAB2", "TAB4", "TAB1"));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A3 |B3 |A1 |B1 |A4 |B4 |\n" +
                        "------------------------\n" +
                        " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                        " 1 | 1 | 2 | 2 | 2 | 2 |\n" +
                        " 2 | 2 | 1 | 1 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 | 2 | 2 |\n" +
                        " 4 | 4 | 1 | 1 | 1 | 1 |\n" +
                        " 4 | 4 | 2 | 2 | 2 | 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSubqueryWithoutCorrelationName() throws Exception {
       String sqlText = "select SUM(SUMMARY) from (\n" +
               " SELECT COUNT(*) AS SUMMARY FROM t1 WHERE k=0\n" +
               " UNION ALL\n" +
               " SELECT COUNT(*) AS SUMMARY FROM t2 WHERE k=0\n" +
               " UNION ALL\n" +
               " SELECT COUNT(*) AS SUMMARY FROM t3 WHERE i=0\n" +
               " )";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected = "1 |\n" +
                    "----\n" +
                    " 3 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    private static void assertUnorderedResult(ResultSet rs, String expectedResult) throws Exception {
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    private static void assertOrderedResult(ResultSet rs, String expectedResult) throws Exception {
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

    private TestConnection conn() {
        return methodWatcher.getOrCreateConnection();
    }


    private void testOrderOfTables(String sqlText, List<String> orderList) throws Exception {
        List<String> resultTableList = new ArrayList<>();

        try(ResultSet resultSet = methodWatcher.executeQuery(sqlText)){
            int i=0;
            while(resultSet.next()){
                String resultString = resultSet.getString(1);
                if (resultString.contains("TableScan")) {
                    int index = resultString.indexOf('[');
                    int endIndex = resultString.indexOf('(');
                    if (index >= 0)
                        resultTableList.add(resultString.substring(index+1, endIndex));
                }
            }

            Assert.assertTrue("Table Join order does not match, expected: " + orderList.toString() + "\nWas: "
                    + resultTableList.toString(), resultTableList.toString().equals(orderList.toString()));
        }
    }
}
