package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
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
public class Subquery_Table_IT {

    private static final String SCHEMA = Subquery_Table_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createdSharedTables() throws Exception {
        classWatcher.executeUpdate("create table T1 (k int, l int)");
        classWatcher.executeUpdate("insert into T1 values (0,1),(0,1),(1,2),(2,3),(2,3),(3,4),(4,5),(4,5),(5,6),(6,7),(6,7),(7,8),(8,9),(8,9),(9,10)");

        classWatcher.executeUpdate("create table T2 (k int, l int)");
        classWatcher.executeUpdate("insert into T2 values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10)");

        classWatcher.executeUpdate("create table T3 (i int)");
        classWatcher.executeUpdate("insert into T3 values (10),(20)");

        classWatcher.executeUpdate("create table T4 (i int)");
        classWatcher.executeUpdate("insert into T4 values (30),(40)");

        TestUtils.executeSqlFile(classWatcher, "test_data/employee.sql", SCHEMA);
        TestUtils.executeSqlFile(classWatcher, "null_int_data.sql", SCHEMA);
        TestUtils.executeSqlFile(classWatcher, "test_data/content.sql", SCHEMA);

        TestUtils.executeSql(classWatcher, "" +
                "create table s (a int, b int, c int, d int, e int, f int);" +
                "insert into s values (0,1,2,3,4,5);" +
                "insert into s values (10,11,12,13,14,15);", SCHEMA);

        TestUtils.executeSql(classWatcher, "" +
                        "create table parentT ( i int, j int, k int); \n" +
                        "create table childT ( i int, j int, k int); \n" +
                        "insert into parentT values (1,1,1), (2,2,2), (3,3,3), (4,4,4); \n" +
                        "insert into parentT select i+4, j+4, k+4 from parentT; \n" +
                        "insert into parentT select i+8, j+8, k+8 from parentT; \n" +
                        "insert into parentT select i+16, j+16, k+16 from parentT; \n" +
                        "insert into parentT select i+32, j+32, k+32 from parentT; \n" +
                        "insert into parentT select i+64, j+64, k+64 from parentT; \n" +
                        "insert into parentT select i+128, j+128, k+128 from parentT; \n" +
                        "insert into parentT select i+256, j+256, k+256 from parentT; \n" +
                        "insert into parentT select i+512, j+512, k+512 from parentT; \n" +
                        "insert into parentT select i+1024, j+1024, k+1024 from parentT; \n" +
                        "insert into childT select * from parentT; \n", SCHEMA
        );
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
    @Test
    public void inSubqueryTooBigToMaterialize() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(*) from parentT where i < 10 and i not in (select i from childT)");
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

    private static void assertUnorderedResult(ResultSet rs, String expectedResult) throws Exception {
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    private static void assertOrderedResult(ResultSet rs, String expectedResult) throws Exception {
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

}