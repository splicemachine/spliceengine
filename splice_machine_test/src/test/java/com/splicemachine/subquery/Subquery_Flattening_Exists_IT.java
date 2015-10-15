package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

public class Subquery_Flattening_Exists_IT {

    private static final String SCHEMA = Subquery_Flattening_Exists_IT.class.getSimpleName();

    private static final int ALL_FLATTENED = 0;

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table EMPTY_TABLE(e1 int, e2 int)");
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int)");
        classWatcher.executeUpdate("create table C(c1 int, c2 int)");
        classWatcher.executeUpdate("create table D(d1 int, d2 int)");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(null,null)");
        classWatcher.executeUpdate("insert into C values            (1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(6,60),(6,60),(null,null)");
        classWatcher.executeUpdate("insert into D values(0,0),(0,0),(1,10),(1,10),              (3,30),(3,30),              (5,50),(5,50),(null,null)");
    }

    @Test
    public void uncorrelated_oneSubqueryTable() throws Exception {
        // subquery reads different table
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select count(*) from A where exists (select b1 from B where b2 > 20)", ALL_FLATTENED, "" +
                        "1 |\n" +
                        "----\n" +
                        " 6 |"
        );
        // subquery reads same table
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select count(*) from A where exists (select a1 from A ai where ai.a2 > 20)", ALL_FLATTENED, "" +
                        "1 |\n" +
                        "----\n" +
                        " 6 |"
        );
        // empty table
        SubqueryITUtil.assertUnorderedResult(conn(), "select count(*) from A where exists (select 1 from EMPTY_TABLE)", ALL_FLATTENED, "" +
                "1 |\n" +
                "----\n" +
                " 0 |"
        );
        // two exists
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "exists (select b1 from B where b2 > 20)" +
                        " and " +
                        "exists (select b1 from B where b1 > 3)", ALL_FLATTENED, "" +
                        "1 |\n" +
                        "----\n" +
                        " 6 |"
        );
        // two exists, different rows
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "exists (select b1 from B where b2 > 20)" +
                        " and " +
                        "exists (select b1 from B where b1 < 0)", ALL_FLATTENED, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
    }

    @Test
    public void uncorrelated_twoSubqueryTables() throws Exception {
        // subquery reads different table
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select A.* from A where exists (select b1 from B join D on b1=d1 where b2 > 20)", ALL_FLATTENED, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
        // subquery reads same table
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select A.* from A where exists (select a1 from A ai join D on a1=d1 where ai.a2 > 20)", ALL_FLATTENED, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
        // two exists
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select A.* from A where " +
                        "exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "exists (select b1 from B join D on b1=d1 where b1 > 3)", ALL_FLATTENED, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
        // two exists, one of which excludes all rows
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "exists (select b1 from B join D on b1=d1 where b1 < 0)", ALL_FLATTENED, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
    }

    @Test
    public void uncorrelated_threeSubqueryTables() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select A.* from A where exists (select 1 from A ai join B on ai.a1=b1 join C on b1=c1 join D on c1=d1)", ALL_FLATTENED, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void correlated_oneSubqueryTable() throws Exception {
        /* simple case */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from D where a1 = d1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* subquery selects constant */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from B where a1 = b1 and b1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* subquery selects b1 */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1 from B where a1 = b1 and b1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* subquery selects b1 multiple times */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1,b1,b1 from B where a1 = b1 and b1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* subquery selects constant multiple times */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1,2,3 from B where a1 = b1 and b1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* subquery selects all */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select * from B where a1 = b1 and b1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
    }

    @Test
    public void correlated_twoSubqueryTables() throws Exception {
        /* no extra predicates */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1 from B join C on b1=c1 where a1 = b1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 3 |\n" +
                        " 4 |\n" +
                        " 5 |"
        );
        /* restriction on C */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1 from B join C on b1=c1 where a1 = b1 and c1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        /* restriction on B and C */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1 from B join C on b1=c1 where a1 = b1 and b2 = 50)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 5 |"
        );
        /* many redundant/identical predicates (we didn't handle this at one point DB-3885) */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "    a1 > 0 and a2 < 50 " +
                        "and exists (select b1 from B join C on b1=c1 where a1 = b1) " +
                        "and a1 > 0 and a2 < 50", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 3 |\n" +
                        " 4 |"
        );
    }

    @Test
    public void correlated_threeSubqueryTables() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from B join C on b1=c1 join D on c1=d1 where a1 = b1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 5 |"
        );
    }

    @Test
    public void correlated_withMultipleCorrelationPredicates() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from D where a1=d1 and a2=d2)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 5 |");
    }

    /* Sometimes correlated column references aren't compared to any table in the subquery. In this case the predicates
     * can simply be moved up (de-correlated).  We have to do this before moving the subquery to a FromSubquery.  */
    @Test
    public void correlated_withCorrelatedColumnRefComparedToConstant() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from B where a1=3)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |");
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select 1 from B where a1=4 and a1=b1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 4 |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multiple exists
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void multipleExistsSubqueries_oneTablePerSubquery() throws Exception {
        // one tables in 2 subqueries
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "exists (select 1 from B where a1=b1 and b1 in (3,5))" +
                        " and " +
                        "exists (select 1 from C where a1=c1 and c1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        // one tables in 3 subqueries
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "exists (select 1 from B where a1=b1)" +
                        " and " +
                        "exists (select 1 from C where a1=c1)" +
                        " and " +
                        "exists (select 1 from D where a1=d1 and d1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
    }

    @Test
    public void multipleExistsSubqueries_twoTablesPerSubquery() throws Exception {
        // one tables in 2 subqueries
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "exists (select 1 from B join C on b1=c1 where a1=b1 and b1 in (3,5))" +
                        " and " +
                        "exists (select 1 from D where a1=d1 and d1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        // one tables in 3 subqueries
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "exists (select 1 from B where a1=b1)" +
                        " and " +
                        "exists (select 1 from C where a1=c1)" +
                        " and " +
                        "exists (select 1 from D where a1=d1 and d1 in (3,5))", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 5 |"
        );
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // exists subqueries with outer joins
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void correlated_rightJoinInSubquery() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select * from C right join D on c1=d1 where a1=c1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 5 |"
        );
    }

    @Test
    public void correlated_leftJoinInSubquery() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select * from C left join D on c1=d1 where a1=c1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 3 |\n" +
                        " 4 |\n" +
                        " 5 |"
        );
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // outer select is join node
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void outerSelectIsJoinNode() throws Exception {
        String sql = "" +
                "select * from A " +
                "join (select * from B" +
                "      where b1 > 0 and exists (select 1 from C where c1=b1)) AS foo on a1=foo.b1";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 1 |10 | 1 |10 |\n" +
                " 1 |10 | 1 |10 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |\n" +
                " 5 |50 | 5 |50 |\n" +
                " 5 |50 | 5 |50 |");
    }

    @Test
    public void outerSelectIsLeftJoinNode() throws Exception {
        String sql = "" +
                "select * from A " +
                "left join (select * from B" +
                "      where b1 > 0 and exists (select 1 from C where c1=b1)) AS foo on a1=foo.b1";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 | B1  | B2  |\n" +
                "--------------------\n" +
                " 0 | 0 |NULL |NULL |\n" +
                " 1 |10 |  1  | 10  |\n" +
                " 1 |10 |  1  | 10  |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 3 |30 |  3  | 30  |\n" +
                " 3 |30 |  3  | 30  |\n" +
                " 4 |40 |  4  | 40  |\n" +
                " 4 |40 |  4  | 40  |\n" +
                " 5 |50 |  5  | 50  |\n" +
                " 5 |50 |  5  | 50  |");
    }

    @Test
    public void outerSelectIsRightJoinNode() throws Exception {
        String sql = "" +
                "select * from A " +
                "right join (select * from B" +
                "      where b1 > 3 and exists (select 1 from C where c1=b1)) AS foo on a1=foo.b1";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |\n" +
                " 5 |50 | 5 |50 |\n" +
                " 5 |50 | 5 |50 |");
    }

    @Test
    public void outerSelectIsJoinNode_subqueryJoinsTwoTables() throws Exception {
        String sql = "" +
                "select A.a1," +
                "       A.a2," +
                "      foo.b1," +
                "      (case when foo.b1 is null then 'NN' else 'YY' end) as \"colAlias\"" +
                " from A " +
                "left outer join (select B.b1 " +
                "                 from B" +
                "                 inner join C on b1=c1" +
                "                 where exists (select 1 from D where c1=d1 and c2 = 30)" +
                "                 and b1 > 0)  AS foo on a1=foo.b1";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 | B1  |colAlias |\n" +
                "------------------------\n" +
                " 0 | 0 |NULL |   NN    |\n" +
                " 1 |10 |NULL |   NN    |\n" +
                " 2 |20 |NULL |   NN    |\n" +
                " 3 |30 |  3  |   YY    |\n" +
                " 3 |30 |  3  |   YY    |\n" +
                " 3 |30 |  3  |   YY    |\n" +
                " 3 |30 |  3  |   YY    |\n" +
                " 4 |40 |NULL |   NN    |\n" +
                " 5 |50 |NULL |   NN    |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // nested
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nestedExists_oneLevel() throws Exception {
        String sql = "select A.* from A where exists(" +
                "select 1 from B where a1=b1 and exists(" +
                "select 1 from C where b1=c1" + "))";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |");
    }

    @Test
    public void nestedExists_twoLevels() throws Exception {
        String sql = "select A.* from A where exists(" +
                "select 1 from B where a1=b1 and exists(" +
                "select 1 from C where b1=c1 and exists(" +
                "select 1 from D where d1=c1" + ")))";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 3 |30 |\n" +
                " 5 |50 |");
        sql = "select A.* from A where exists(" +
                "select 1 from B where a1=b1 and a1=5 and exists(" +
                "select 1 from C where b1=c1 and b1=5 and exists(" +
                "select 1 from D where d1=c1 and d1=5" + ")))";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, ALL_FLATTENED, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 5 |50 |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // union
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void union_unCorrelated() throws Exception {
        // union of empty tables
        String sql = "select * from A where exists(select 1 from EMPTY_TABLE union select 1 from EMPTY_TABLE)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "");

        // union one non-empty first subquery
        sql = "select count(*) from A where exists(select 1 from C union select 1 from EMPTY_TABLE)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "1 |\n" +
                "----\n" +
                " 6 |");

        // union one non-empty second subquery
        sql = "select count(*) from A where exists(select 1 from EMPTY_TABLE union select 1 from C )";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "1 |\n" +
                "----\n" +
                " 6 |");
    }

    @Test
    public void union_correlated() throws Exception {
        // union of empty tables
        String sql = "select * from A where exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from EMPTY_TABLE e where e1=a1)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "");

        // union one non-empty first subquery
        sql = "select * from A where exists(select 1 from C where c1=a1 union select 1 from EMPTY_TABLE where e1=a1)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |");

        // union one non-empty second subquery
        sql = "select * from A where exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from C where c1=a1)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |");

        // union no non-empty
        sql = "select * from A where exists(select 1 from D where d1=a1 union select 1 from C where c1=a1)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |");

        // union same table
        sql = "select * from A where exists(select 1 from D where d1=a1 union select 1 from D where d2=a2)";
        SubqueryITUtil.assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 3 |30 |\n" +
                " 5 |50 |");
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // misc other tests
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * TEST: from A where EXISTS(B join C and B.b1=A.a1)
     *
     * Where there are multiple rows in B for every row in A. Where there is at least one row in C for every set of rows
     * from B for a given A row.  But there is not a row in C for EVERY row in B.
     *
     * We expect every row in A to be selected.
     *
     * We only get the correct answer reliably if we do not filter out rows in B before joining C.
     */
    @Test
    public void existsJoinWithNonMatchingIntermediateRows() throws Exception {
        methodWatcher.executeUpdate("create table AA(a1 int)");
        methodWatcher.executeUpdate("create table BB(b1 int, b2 int)");
        methodWatcher.executeUpdate("create table CC(c1 int)");
        methodWatcher.executeUpdate("insert into AA values(1),(2)");
        methodWatcher.executeUpdate("insert into BB values(1, null),(1,1),(1,1),(2,2),(2,null)");
        methodWatcher.executeUpdate("insert into CC values(1),(2)");

        /* correlated */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from AA where exists(select 1 from BB join CC on b2=c1 where b1=a1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |");
        /* uncorrelated */
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from AA where exists(select 1 from BB join CC on b2=c1)", ALL_FLATTENED, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |");
    }


    /* Tests from derby's 'ExistsWithSubqueriesTest' */
    @Test
    public void apacheDerbyTests() throws Exception {
        methodWatcher.executeUpdate("create table empty (i int)");
        methodWatcher.executeUpdate("create table onerow (j int)");
        methodWatcher.executeUpdate("insert into onerow values 2");
        methodWatcher.executeUpdate("create table diffrow (k int)");
        methodWatcher.executeUpdate("insert into diffrow values 4");

        String expRS = "" +
                "J |\n" +
                "----\n" +
                " 2 |";

        checkQuery(expRS, "select j from onerow where exists (" +
                "select 1 from diffrow where 1 = 1 union " +
                "select * from diffrow where onerow.j < k)");

        // Right child of UNION has qualified "*" for RCL and references table from outer query.
        checkQuery(expRS, "select j from onerow where exists (" +
                "select 1 from diffrow where 1 = 1 union " +
                "select diffrow.* from diffrow where onerow.j < k)");

        // Right child of UNION has explicit RCL and references table from outer query.
        checkQuery(expRS, "select j from onerow where exists (" +
                "select 1 from diffrow where 1 = 1 union " +
                "select k from diffrow where onerow.j < k)");


        // Expect 0 rows for the following.  Similar queries to above except modified to return no rows.
        checkQuery("", "select j from onerow where exists (" +
                "select 1 from diffrow where 1 = 0 union " +
                "select * from diffrow where onerow.j > k)");
    }

    private void checkQuery(String expRS, String query) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        assertEquals(expRS, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // UN-FLATTENED exists queries
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void notFlattened_or() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "exists (select b1 from B where a1=b1 and b1 in (3,5))" +
                        "or " +
                        "exists (select b1 from B where a1=b1 and b1 in (0,3,5))", 2, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 3 |\n" +
                        " 5 |"
        );
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select a1 from A where exists (select b1 from B where a1=b1 and b1 < 3) or a1=5", 1, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 5 |"
        );

    }

    @Test
    public void notFlattened_havingSubquery() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having sum(b1) > 0 and exists(select 1 from C)", 1, "" +
                        "B1 | 2  |\n" +
                        "---------\n" +
                        " 2 |40  |\n" +
                        " 3 |60  |\n" +
                        " 4 |80  |\n" +
                        " 5 |100 |"
        );
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having exists(select 1 from C)", 1, "" +
                        "B1 | 2  |\n" +
                        "---------\n" +
                        " 2 |40  |\n" +
                        " 3 |60  |\n" +
                        " 4 |80  |\n" +
                        " 5 |100 |"
        );
    }

    @Test
    public void notFlattened_multiLevelCorrelationPredicate() throws Exception {
        SubqueryITUtil.assertUnorderedResult(conn(),
                "select A.* from A where " +
                        "exists(select 1 from B where a1=b1 and " +
                        "exists(select 1 from C where c1=a1))", 2, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );

    }

    private TestConnection conn() {
        return methodWatcher.getOrCreateConnection();
    }
}