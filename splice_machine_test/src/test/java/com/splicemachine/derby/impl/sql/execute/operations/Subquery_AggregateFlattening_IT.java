package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Joiner;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Test for flattening where-subqueries containing aggregates. Splice added this capability. <p/> All of the tested
 * subqueries should be in the where clause and should have aggregates.
 */
public class Subquery_AggregateFlattening_IT {

    private static final String SCHEMA = Subquery_AggregateFlattening_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        //
        // These are the tables used for correctness tests in this class.
        //
        classWatcher.executeUpdate("create table A(a1 int, a2 int, a3 int)");
        classWatcher.executeUpdate("insert into A values(0,0,0),(1,10,10),(2,20,20),(3,30,30),(4,40,40),(5,50,500),(5,5,5)");

        classWatcher.executeUpdate("create table B(b1 int, b2 int, b3 int)");
        classWatcher.executeUpdate("insert into B values" +
                "(0,0,0)," +
                "(1,1,1)," +
                "(2,2,2),(2,20, 200)," +
                "(3,3,3),(3,30, 300),(3,300, 3000)," +
                "(4,4,4),(4,40, 400),(4,400, 4000),(4,4000, 40000)," +
                "(5,5000,5)");
    }

    @Test
    public void subquery_equals_columnRef() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |");
        sql = "select * from A where A.a2  = (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |");
    }

    @Test
    public void subquery_notEquals_columnRef() throws Exception {
        String sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void subquery_notEquals_extraTopLevelPredicate() throws Exception {
        // the subquery filters out A.a1=0,2 and the top level predicate filters out A.a1=3
        String sql = "select * from A where A.a2!= 30 AND A.a2 != (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void subquery_lessThan_extraPredicateInSubquery() throws Exception {
        String sql = "select * from A where A.a2  < (select sum(b2) from B where B.b1=A.a1 AND B.b2 >= 40)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void subquery_greaterThanLessThan_columnRef() throws Exception {
        String sql = "select * from A where A.a2  > (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |");
        sql = "select * from A where A.a2  < (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void subquery_equals_arithmetic() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 3 |30 |30 |");
    }

    @Test
    public void subquery_equals_constant() throws Exception {
        String sql = "select * from A where 4444 = (select sum(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
        sql = "select * from A where (select sum(b2) from B where B.b1=A.a1) = 4444";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void subquery_aggregateMultipliedByConstant() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2)*0.1 from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 0 | 0 | 0  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void subquery_correlatedAggregateInMiddle() throws Exception {
        for(int i=0;i<20;i++) {
            List<String> preds = Arrays.asList("b1>0", "b1=a1", "b2>0", "b3<=0");
            //List<String> preds = Arrays.asList("b1>0", "b1=a1", "b3=a3", "b2>0", "b3<=0");
            Collections.shuffle(preds);
            com.google.common.base.Joiner andJoiner = Joiner.on(" AND ");
            String subquery = "select max(b2) from B where " + andJoiner.join(preds);
            String sql = "select * from A where A.a2 < (" + subquery + ")";
            System.out.println("SQL: " + sql);
            assertUnorderedResult(sql, 0, "");
        }
    }

    @Test
    public void subquery_unaryOperatorArithmeticNode() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1 and sqrt(b1) > 1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multiple correlated predicates in same subquery
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void subquery_multipleCorrelationPredicates() throws Exception {
        String sql = "select * from A where A.a2 > (select sum(b2) from B where A.a1=B.b1 AND A.a1=B.b3)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 4 |40 |40 |");

        sql = "select * from A where A.a2 < (select sum(b2) from B where A.a1=B.b1 AND A.a3=B.b3)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 5 | 5 | 5 |");
    }

    @Test
    public void subquery_multipleCorrelationPredicates_Extraneous() throws Exception {
        // Extraneous correlated predicates.
        String sql = "select * from A where A.a2 < (select sum(b2) from B where B.b1=A.a1 AND B.b1=A.a1 AND B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 2 |20 |20  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // flattening multiple subqueries
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void flatteningMultipleSubqueries() throws Exception {
        String sql = "select * from A where 4444 = (select sum(b2) from B where B.b1=A.a1)" +
                "           or  333 = (select sum(b2) from B where B.b1=A.a1)" +
                "           or   22 = (select sum(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 4 |40 |40 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // uncorrelated aggregates subqueries - we DO flatten these as well
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void uncorrelatedAggregateSubquery() throws Exception {
        String sql = "select * from A where a2 < (select sum(b1) from B)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 1 |10 |10 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 5 | 5 | 5 |");
    }

    @Test
    public void uncorrelatedAggregateSubquery2() throws Exception {
        String sql = "select * from A where a2 > (select sum(b1) from B)" +
                "                       or  a3 > (select sum(b2) from B)";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // nested
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nested_uncorrelated_two() throws Exception {
        String sql = "select * from A where a2 > " +
                "              (select sum(b1) from B where b2 > " +
                "                  (select sum(a1) from A))";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void nested_uncorrelated_three() throws Exception {
        String sql = "select * from A where a2 > " +
                "              (select sum(b1) from B where b2 > " +
                "                  (select sum(a1) from A where a1 < " +
                "                      (select count(b1) from B)))";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void nested_uncorrelated_four() throws Exception {
        String sql = "select * from A where a2 > " +
                "              (select sum(b1) from B where b2 > " +
                "                  (select sum(a1) from A where a1 < " +
                "                      (select count(b1) from B where b2 > " +
                "                          (select max(a3) from A))))";
        assertUnorderedResult(sql, 0, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // unsupported -- aggregate subqueries we don't flatten yet
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void unsupported_multipleCorrelationPredicatesWhereOneReferencesIntermediateAggregationResults() throws Exception {
        /* We can't flatten this, assert that we don't attempt to and cause correctness problems.
         *
         * The problem here is that the subquery has a correlated predicate that involves B.b2 where B.b2 is the
         * aggregated column in the subquery. It is impossible to calculate the aggregates once and join with A
         * when each aggregate (each sum) depends on the row of A to which it will be joined.
         */
        String sql = "select * from A where A.a2 = (select sum(b2) from B where B.b1=A.a1 AND A.a2=B.b2)";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void unsupported_groupByInSubquery() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1 group by b1)";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |");
    }

    @Test
    public void unsupported_complexAggregateColumnExpression() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1 and B.b1=A.a1 or B.b1=A.a1)";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |");
    }

    @Test
    public void unsupported_v() throws Exception {
        String sql = "select * from A where A.a2 = (select max(40) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void unsupported_complexWhere() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where not (B.b1=A.a1*10 or B.b1=A.a1))";
        assertUnorderedResult(sql, 1, "");
    }

    @Test
    public void unsupported_inNotIn() throws Exception {
        // The important part for this test is that we don't attempt to flatten currently.  But incidentally this
        // first query returns the wrong result.  DB-3599
        String sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1 and B.b3 in (3,30,300))";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 2 |20 |20  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
        sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1 and B.b3 not in (3,30,300))";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void unsupported_aggregateColumnMultipliedByConstant() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2*0.1) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 0 | 0 | 0  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void unsupported_nestedCorrelatedTwoLevels() throws Exception {
        String sql = "select t1.* from A t1 where t1.a2 > " +
                "              (select sum(b1) from B t2 where t2.b2 > " +
                "                  (select sum(a1) from A t3 where t3.a2=t2.b1))";
        assertUnorderedResult(sql, 1, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 2 |20 |20  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void unsupported_nestedCorrelatedThreeLevels() throws Exception {
        String sql = "select t1.* from A t1 where t1.a2 > " +
                "              (select sum(b1) from B t2 where t2.b2 > " +
                "                  (select sum(a1) from A t3 where t3.a1=t1.a1))";
        assertUnorderedResult(sql, 2, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // performance
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * The timeout here is an important part of the test. This query runs for approximately 3 minutes on an OSX laptop
     * when this subquery is NOT executed as a join.  When it is flattened it runs in about 2 seconds.  We assert that
     * it runs in less than 40 seconds to allow for slower execution on Jenkins VMs.
     */
    @Test(timeout = 40_000)
    public void subqueryExecutionSpeedTest() throws Exception {
        classWatcher.executeUpdate("create table P1 (a1 int, a2 int)");
        classWatcher.executeUpdate("create table P2 (b1 int)");
        classWatcher.executeUpdate("create table P3 (c1 int, c2 int)");
        classWatcher.executeUpdate("create table P4 (d1 int)");

        classWatcher.executeUpdate("insert into P1 values(1,1),(2,2),(3,3),(4,4),(4,100)");
        classWatcher.executeUpdate("insert into P2 values(1),(2),(3),(4)");
        classWatcher.executeUpdate("insert into P3 values(1,1),(2,2),(3,100),(4, 100)");
        classWatcher.executeUpdate("insert into P4 values(1),(2),(3),(4)");

        // We end up with about 300 rows in each table.
        for (int i = 0; i < 6; i++) {
            classWatcher.executeUpdate("insert into P1 select a1+1,a2+1 from P1");
            classWatcher.executeUpdate("insert into P2 select b1+1 from P2");
            classWatcher.executeUpdate("insert into P3 select c1+1,c2+1 from P3");
            classWatcher.executeUpdate("insert into P4 select d1+1 from P4");
        }

        ResultSet rs = methodWatcher.executeQuery("" +
                "select a1,b1 " +
                "from P1 " +
                "join P2 on P1.a1=P2.b1  " +
                "where  " +
                "P1.a2 = (select max(c2) " +
                "        from " +
                "        P3 " +
                "        join P4 on P3.c1=P4.d1 " +
                "        where P3.c1 = P1.a1)");

        assertEquals(51, SpliceUnitTest.resultSetSize(rs));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Assert that the query executes with the expected result and that it executes using the expected number of
     * subqueries
     */
    private void assertUnorderedResult(String query, int expectedSubqueryCountInPlan, String expectedResult) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));

        ResultSet rs2 = methodWatcher.executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs2);
        assertEquals(expectedSubqueryCountInPlan, countSubqueriesInPlan(explainPlanText));
    }

    /**
     * Counts the number of Subquery nodes that appear in the explain plan text for a given query.
     */
    private static int countSubqueriesInPlan(String a) {
        Pattern pattern = Pattern.compile("^.*?Subquery\\s+\\(", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
        int count = 0;
        Matcher matcher = pattern.matcher(a);
        while (matcher.find()) {
            count++;

        }
        return count;
    }

}
