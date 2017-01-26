/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import org.spark_project.guava.base.Joiner;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.subquery.SubqueryITUtil.ZERO_SUBQUERY_NODES;
import static org.junit.Assert.assertEquals;

/**
 * Test for flattening where-subqueries containing aggregates. Splice added this capability. <p/> All of the tested
 * subqueries should be in the where clause and should have aggregates.
 */
public class Subquery_Flattening_Aggregate_IT {

    private static final String SCHEMA = Subquery_Flattening_Aggregate_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int, a3 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int, b3 int)");
        classWatcher.executeUpdate("create table C(c1 int, c2 int, c3 int)");

        classWatcher.executeUpdate("insert into A values(0,0,0),(1,10,10),(2,20,20),(3,30,30),(4,40,40),(5,50,500),(5,5,5)");
        classWatcher.executeUpdate("insert into B values" +
                "(0,0,0)," +
                "(1,1,1)," +
                "(2,2,2),(2,20, 200)," +
                "(3,3,3),(3,30, 300),(3,300, 3000)," +
                "(4,4,4),(4,40, 400),(4,400, 4000),(4,4000, 40000)," +
                "(5,5000,5)");
        classWatcher.executeUpdate("insert into C values(1,1,1),(1,1,1),(3,3,3),(3,3,3),(5,5,5),(5,5,5),(7,7,7),(7,7,7)");
    }

    @Test
    public void equals_columnRef() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |");
        sql = "select * from A where A.a2  = (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |");
    }

    @Test
    public void notEquals_columnRef() throws Exception {
        String sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");

        // with extra predicates on outer table
        sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1) and a1 = 5 or a2 = 30";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void notEquals_extraTopLevelPredicate() throws Exception {
        // the subquery filters out A.a1=0,2 and the top level predicate filters out A.a1=3
        String sql = "select * from A where A.a2!= 30 AND A.a2 != (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void lessThan_extraPredicateInSubquery() throws Exception {
        String sql = "select * from A where A.a2  < (select sum(b2) from B where B.b1=A.a1 AND B.b2 >= 40)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void greaterThanLessThan_columnRef() throws Exception {
        String sql = "select * from A where A.a2  > (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |");
        sql = "select * from A where A.a2  < (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void equals_arithmetic() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 3 |30 |30 |");
    }

    @Test
    public void equals_constant() throws Exception {
        String sql = "select * from A where 4444 = (select sum(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
        sql = "select * from A where (select sum(b2) from B where B.b1=A.a1) = 4444";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void aggregateMultipliedByConstant() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2)*0.1 from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 0 | 0 | 0  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void correlatedAggregateInMiddle() throws Exception {
        for (int i = 0; i < 20; i++) {
            List<String> preds = Arrays.asList("b1>0", "b1=a1", "b2>0", "b3<=0");
            Collections.shuffle(preds);
            String subquery = "select max(b2) from B where " + Joiner.on(" AND ").join(preds);
            String sql = "select * from A where A.a2 < (" + subquery + ")";
            assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "");
        }
    }

    @Test
    public void unaryOperatorArithmeticNode() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where B.b1=A.a1 and sqrt(b1) > 1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |");
    }

    @Test
    public void outerQueryIsAggregate() throws Exception {
        String sql = "select 2*sum(a1),3*sum(a2),4*sum(a3) from A where A.a2 < (select .2*max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "1 | 2  |  3  |\n" +
                "---------------\n" +
                "34 |375 |2300 |");
    }

    @Test
    public void outerQueryIsAggregateAndJoinOnSubqueryTable() throws Exception {
        String sql = "" +
                "select 2*sum(a1),3*sum(a2),4*sum(a3),5*sum(b1) " +
                "from A " +
                "join B on A.a1 = B.b1 " +
                "where A.a2 < (select .2*max(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "1 | 2  |  3  | 4  |\n" +
                "--------------------\n" +
                "70 |915 |3020 |175 |");

        //
        sql = "" +
                "select 2*sum(a1),3*sum(a2),4*sum(a3),5*sum(b1) " +
                "from A " +
                "join B on A.a1 = B.b1 " +
                "where b.b2 < (select .2*avg(bbb.b2) from B bbb where bbb.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "1 | 2  | 3  | 4 |\n" +
                "------------------\n" +
                "26 |390 |520 |65 |");
    }

    /* At one point this shape of query was not flattened, it now is DB-3599 */
    @Test
    public void inNotIn() throws Exception {
        String sql;
        sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1 and B.b3 in (3,30,300))";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "");
        sql = "select * from A where A.a2 != (select max(b2) from B where B.b1=A.a1 and B.b3 not in (3,30,300))";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multiple correlated predicates in same subquery
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void multipleCorrelationPredicates() throws Exception {
        String sql = "select * from A where A.a2 > (select sum(b2) from B where A.a1=B.b1 AND A.a1=B.b3)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 4 |40 |40 |");

        sql = "select * from A where A.a2 < (select sum(b2) from B where A.a1=B.b1 AND A.a3=B.b3)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 5 | 5 | 5 |");
    }

    @Test
    public void multipleCorrelationPredicates_Extraneous() throws Exception {
        // Extraneous correlated predicates.
        String sql = "select * from A where A.a2 < (select sum(b2) from B where B.b1=A.a1 AND B.b1=A.a1 AND B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
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
    public void multipleSubqueries() throws Exception {
        // or
        String sql = "select * from A where" +
                "              4444 = (select sum(b2) from B where B.b1=A.a1)" +
                "           or  333 = (select sum(b2) from B where B.b1=A.a1)" +
                "           or   22 = (select sum(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |\n" +
                " 4 |40 |40 |");

        // and
        sql = "select * from A where" +
                "                4444 = (select sum(b2) from B where B.b1=A.a1)" +
                "           and  4000 = (select max(b2) from B where B.b1=A.a1)" +
                "           and     4 = (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");

        // and - with CR on opposite sides
        sql = "select * from A where" +
                "                4444 = (select sum(b2) from B where B.b1=A.a1)" +
                "           and  (select max(b2) from B where B.b1=A.a1) = 4000" +
                "           and     4 = (select min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");

        // and with non subquery predicates
        sql = "select * from A where" +
                "                4444 = (select sum(b2) from B where B.b1=A.a1)" +
                "           and  4000 = (select max(b2) from B where B.b1=A.a1)" +
                "           and     4 = (select min(b2) from B where B.b1=A.a1)" +
                "           and a2 >= 40 and a3 <= 40 and a1 between 1 and 4";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void multipleSubqueries_allAgainstOuterTable() throws Exception {
        String sql = "select * from A where" +
                "                30 = (select sum(aa.a2) from A aa where aa.a1=A.a1)" +
                "           and  30 = (select avg(aa.a2) from A aa where aa.a1=A.a1)" +
                "           and  30 = (select min(aa.a2) from A aa where aa.a1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 3 |30 |30 |");
    }

    @Test
    public void multipleSubqueries_allDistinctTables() throws Exception {
        String sql = "select * from A where" +
                "              5000 = (select sum(b2) from B where B.b1=A.a1)" +
                "            and  5 = (select avg(c2) from C where C.c1=A.a1)";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // uncorrelated aggregates subqueries - we DO flatten these as well
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void uncorrelatedAggregateSubquery() throws Exception {
        String sql = "select * from A where a2 < (select sum(b1) from B)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
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
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
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
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
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
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
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
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void nestedCorrelatedTwoLevels() throws Exception {
        String sql = "select t1.* from A t1 where t1.a2 > " +
                "              (select sum(b1) from B t2 where t2.b2 > " +
                "                  (select sum(a1) from A t3 where t3.a2=t2.b1))";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 2 |20 |20  |\n" +
                " 3 |30 |30  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // outer query has multiple tables
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void multiTableOuterQuery() throws Exception {
        String sql = "" +
                "select * from A " +
                " join B on a1=b1 " +
                " where a2 > (select min(c2) from C where C.c1=A.a1) " +
                " and b3 < 1000";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |B1 | B2  |B3  |\n" +
                "----------------------------\n" +
                " 1 |10 |10  | 1 |  1  | 1  |\n" +
                " 3 |30 |30  | 3 |  3  | 3  |\n" +
                " 3 |30 |30  | 3 | 30  |300 |\n" +
                " 5 |50 |500 | 5 |5000 | 5  |");
    }

    @Test
    public void multiTableOuterQuery_withMultipleSubqueries() throws Exception {
        String sql = "" +
                "select * from A " +
                " join B on a1=b1 " +
                " where b3 < 1000 " +
                " and a2 > (select min(c2) from C where C.c1=A.a1) " +
                " and b3 > (select avg(c2) from C where C.c1=A.a1) ";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |B1 |B2 |B3  |\n" +
                "-------------------------\n" +
                " 3 |30 |30 | 3 |30 |300 |");
    }


    @Test
    public void multiTableOuterQuery_withMultipleSubqueries_oneOfThemFromOuterQuery() throws Exception {
        String sql = "" +
                "select * from A " +
                " join B on a1=b1 " +
                " where b3 < 1000 " +
                " and a2 > (select min(c2) from C where C.c1=A.a1) " +
                " and b3 > (select avg(bb.b2) from B bb where bb.b1=A.a1) ";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |B1 |B2 |B3  |\n" +
                "-------------------------\n" +
                " 3 |30 |30 | 3 |30 |300 |");
    }

    @Test
    public void multiTableOuterQuery_withMultipleMultiTableSubqueries() throws Exception {
        String sql = "" +
                "select * from A " +
                " join B on a1=b1 " +
                " where a1 > 0" +
                " and a2 > (select min(cc.c2) from C cc join B bb on cc.c1=bb.b1 where cc.c1=A.a1 and cc.c1 > 0) " +
                " and b3 > (select avg(cc.c2) from C cc join B bb on cc.c1=bb.b1 where cc.c1=A.a1 and bb.b2 > 0) ";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |B1 |B2  | B3  |\n" +
                "---------------------------\n" +
                " 3 |30 |30 | 3 |30  | 300 |\n" +
                " 3 |30 |30 | 3 |300 |3000 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // misc
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void aggregateOfConstant() throws Exception {
        String sql = "select * from A where A.a2 = (select max(40) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 4 |40 |40 |");
    }

    @Test
    public void aggregateColumnMultipliedByConstant() throws Exception {
        String sql = "select * from A where A.a2 * 10 = (select max(b2*0.1) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 0 | 0 | 0  |\n" +
                " 4 |40 |40  |\n" +
                " 5 |50 |500 |");
    }

    @Test
    public void multipleAggregateFunctions() throws Exception {
        // one distinct column reference in aggregate expression
        String sql = "select * from A where A.a2 > (select max(b2) - min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 2 |20 |20  |\n" +
                " 5 | 5 | 5  |\n" +
                " 5 |50 |500 |");
        // two distinct column references in aggregate expression
        sql = "select * from A where A.a2 > (select max(b3) - min(b2) from B where B.b1=A.a1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |10  |\n" +
                " 5 | 5 | 5  |\n" +
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
    public void unsupported_complexWhere() throws Exception {
        String sql = "select * from A where A.a2 = (select max(b2) from B where not (B.b1=A.a1*10 or B.b1=A.a1))";
        assertUnorderedResult(sql, 1, "");
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

    private void assertUnorderedResult(String sql, int expectedSubqueryCountInPlan, String expectedResult) throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                sql, expectedSubqueryCountInPlan, expectedResult
        );
    }

}
