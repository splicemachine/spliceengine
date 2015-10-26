package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;

import static com.splicemachine.subquery.SubqueryITUtil.*;
import static org.junit.Assert.assertEquals;

public class Subquery_Flattening_NotExists_IT {

    private static final String SCHEMA = Subquery_Flattening_NotExists_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table EMPTY_TABLE(e1 int, e2 int, e3 int, e4 int)");
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int)");
        classWatcher.executeUpdate("create table C(c1 int, c2 int)");
        classWatcher.executeUpdate("create table D(d1 int, d2 int)");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50),(7,70)");
        classWatcher.executeUpdate("insert into B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(null,null)");
        classWatcher.executeUpdate("insert into C values            (1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(6,60),(6,60),(null,null)");
        classWatcher.executeUpdate("insert into D values(0,0),(0,0),(1,10),(1,10),              (3,30),(3,30),              (5,50),(5,50),(null,null),(7,null)");
    }

    @Test
    public void uncorrelated_oneSubqueryTable() throws Exception {
        // subquery reads different table
        assertUnorderedResult(conn(),
                "select count(*) from A where NOT exists (select b1 from B where b2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
        // subquery reads same table
        assertUnorderedResult(conn(),
                "select count(*) from A where NOT exists (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
        // empty table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from EMPTY_TABLE)", ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |\n" +
                " 7 |70 |"
        );
        // two exists
        assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "NOT exists (select b1 from B where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B where b1 > 3)", ZERO_SUBQUERY_NODES, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
        // two exists, different rows
        assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "NOT exists (select b1 from B where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B where b1 < 0)", ZERO_SUBQUERY_NODES, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
    }


    @Test
    public void uncorrelated_twoSubqueryTables() throws Exception {
        // subquery reads different table
        assertUnorderedResult(conn(),
                "select A.* from A where NOT exists (select b1 from B join D on b1=d1 where b2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        ""
        );
        // subquery reads same table
        assertUnorderedResult(conn(),
                "select A.* from A where NOT exists (select a1 from A ai join D on a1=d1 where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        ""
        );
        // two exists
        assertUnorderedResult(conn(),
                "select A.* from A where " +
                        "NOT exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B join D on b1=d1 where b1 > 3)", ZERO_SUBQUERY_NODES, "" +
                        ""
        );
        // two exists, one of which excludes all rows
        assertUnorderedResult(conn(),
                "select count(*) from A where " +
                        "NOT exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B join D on b1=d1 where b1 < 0)", ZERO_SUBQUERY_NODES, "" +
                        "1 |\n" +
                        "----\n" +
                        " 0 |"
        );
    }

    @Test
    public void uncorrelated_threeSubqueryTables() throws Exception {
        assertUnorderedResult(conn(),
                "select A.* from A where NOT exists (select 1 from A ai join B on ai.a1=b1 join C on b1=c1 join D on c1=d1)", ZERO_SUBQUERY_NODES, "");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // correlated
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void correlated_oneSubqueryTable() throws Exception {
        /* simple case */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from D where a1 = d1)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 2 |\n" +
                        " 4 |"
        );
        /* subquery selects constant */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B where a1 = b1 and b1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* subquery selects b1 */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select b1 from B where a1 = b1 and b1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* subquery selects b1 multiple times */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select b1,b1,b1 from B where a1 = b1 and b1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* subquery selects constant multiple times */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1,2,3 from B where a1 = b1 and b1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* subquery selects all */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select * from B where a1 = b1 and b1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* not exists with null on both sides */
        assertUnorderedResult(conn(),
                "select * from B where NOT exists (select * from C where b1 = c1)", ZERO_SUBQUERY_NODES, "" +
                        "B1  | B2  |\n" +
                        "------------\n" +
                        "  0  |  0  |\n" +
                        "  0  |  0  |\n" +
                        "NULL |NULL |"
        );
    }

    @Test
    public void correlated_twoSubqueryTables() throws Exception {
        /* no extra predicates */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1 = b1)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 7 |"
        );
        /* restriction on C */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1 = b1 and c1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        /* restriction on B and C */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1 = b1 and b2 = 50)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 3 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
    }

    @Test
    public void correlated_threeSubqueryTables() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 join D on c1=d1 where a1 = b1)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
    }

    @Test
    public void correlated_withMultipleCorrelationPredicates() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where not exists (select 1 from D where a1=d1 and a2=d2)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |");
    }

    /* We do flatten these for EXISTS. But not for NOT EXISTS.  See notes on why in ExistsSubqueryWhereVisitor.java*/
    @Test
    public void correlated_withCorrelatedColumnRefComparedToConstant() throws Exception {
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where a1=3)", 1, "" +
                "A1 |\n" +
                "----\n" +
                " 0 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 7 |"
        );
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where a1=4 and a1=b1)", 1, "" +
                "A1 |\n" +
                "----\n" +
                " 0 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |\n" +
                " 5 |\n" +
                " 7 |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multiple NOT exists
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void multipleExistsSubqueries_oneTablePerSubquery() throws Exception {
        // one tables in 2 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B where a1=b1 and b1 in (3,5))" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1 and c1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        // one tables in 3 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1)" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1 and d1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 7 |");
    }

    @Test
    public void multipleExistsSubqueries_twoTablesPerSubquery() throws Exception {
        // one tables in 2 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B join C on b1=c1 where a1=b1 and b1 in (3,5))" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1 and d1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        // one tables in 3 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1)" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1 and d1 in (3,5))", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 7 |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // NOT exists subqueries with outer joins
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void correlated_rightJoinInSubquery() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select * from C right join D on c1=d1 where a1=c1)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
    }

    @Test
    public void correlated_leftJoinInSubquery() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select * from C left join D on c1=d1 where a1=c1)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 7 |"
        );
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // outer select is join node
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void outerSelectIsJoinNode() throws Exception {
        String sql = "select * from A join (select * from B where b1 > 0 and NOT exists (select 1 from D where d1=b1)) AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |");
    }

    @Test
    public void outerSelectIsLeftJoinNode() throws Exception {
        String sql = "select * from A left join (select * from B where b1 > 0 and NOT exists (select 1 from D where d1=b1)) AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 | B1  | B2  |\n" +
                "--------------------\n" +
                " 0 | 0 |NULL |NULL |\n" +
                " 1 |10 |NULL |NULL |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 3 |30 |NULL |NULL |\n" +
                " 4 |40 |  4  | 40  |\n" +
                " 4 |40 |  4  | 40  |\n" +
                " 5 |50 |NULL |NULL |\n" +
                " 7 |70 |NULL |NULL |");
    }

    @Test
    public void outerSelectIsRightJoinNode() throws Exception {
        String sql = "select * from A right join (select * from B where b1 > 3 and NOT exists (select 1 from D where d1=b1)) AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |");
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
                "                 where NOT exists (select 1 from D where c1=d1)" +
                "                 and b1 > 0)  AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 | B1  |colAlias |\n" +
                "------------------------\n" +
                " 0 | 0 |NULL |   NN    |\n" +
                " 1 |10 |NULL |   NN    |\n" +
                " 2 |20 |  2  |   YY    |\n" +
                " 2 |20 |  2  |   YY    |\n" +
                " 2 |20 |  2  |   YY    |\n" +
                " 2 |20 |  2  |   YY    |\n" +
                " 3 |30 |NULL |   NN    |\n" +
                " 4 |40 |  4  |   YY    |\n" +
                " 4 |40 |  4  |   YY    |\n" +
                " 4 |40 |  4  |   YY    |\n" +
                " 4 |40 |  4  |   YY    |\n" +
                " 5 |50 |NULL |   NN    |\n" +
                " 7 |70 |NULL |   NN    |");
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // nested
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nestedExists_oneLevel() throws Exception {
        String sql = "select A.* from A where NOT exists(" +
                "select 1 from B where a1=b1 and NOT exists(" +
                "select 1 from C where b1=c1" + "))";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |\n" +
                " 7 |70 |"
        );
    }

    @Test
    public void nestedExists_twoLevels() throws Exception {
        String sql = "select A.* from A where NOT exists(" +
                "select 1 from B where a1=b1 and NOT exists(" +
                "select 1 from C where b1=c1 and NOT exists(" +
                "select 1 from D where d1=c1" + ")))";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 2 |20 |\n" +
                " 4 |40 |\n" +
                " 7 |70 |");
        sql = "select A.* from A where NOT exists(" +
                "select 1 from B where a1=b1 and b1=5 and NOT exists(" +
                "select 1 from C where b1=c1 and c1=5 and NOT exists(" +
                "select 1 from D where d1=c1 and d1=5" + ")))";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 7 |70 |");
    }



    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // misc other tests
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void notExistsJoinWithNonMatchingIntermediateRows() throws Exception {
        methodWatcher.executeUpdate("create table AA(a1 int)");
        methodWatcher.executeUpdate("create table BB(b1 int, b2 int)");
        methodWatcher.executeUpdate("create table CC(c1 int)");
        methodWatcher.executeUpdate("insert into AA values(1),(2)");
        methodWatcher.executeUpdate("insert into BB values(1, null),(1,1),(1,1),(2,2),(2,null)");
        methodWatcher.executeUpdate("insert into CC values(1),(2)");

        /* correlated */
        assertUnorderedResult(conn(), "select a1 from AA where NOT exists(select 1 from BB join CC on b2=c1 where b1=a1)", ZERO_SUBQUERY_NODES, "");
        /* uncorrelated */
        assertUnorderedResult(conn(), "select a1 from AA where NOT exists(select 1 from BB join CC on b2=c1)", ZERO_SUBQUERY_NODES, "");
    }

    @Test
    public void multipleOuterTables_withExplicitJoin() throws Exception {
        String sql = "select * from A join B on a1=b1 where NOT exists (select 1 from C where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 0 | 0 | 0 | 0 |\n" +
                " 0 | 0 | 0 | 0 |");
        // two subquery tables
        sql = "select * from A join B on a1=b1 where NOT exists (select 1 from C join D on c1=d1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 0 | 0 | 0 | 0 |\n" +
                " 0 | 0 | 0 | 0 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |");
        // different two outer tables, right join
        sql = "select * from A right join D on a1=d1 where NOT exists (select 1 from C join B on c1=b1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  | D1  | D2  |\n" +
                "------------------------\n" +
                "  0  |  0  |  0  |  0  |\n" +
                "  0  |  0  |  0  |  0  |\n" +
                "  7  | 70  |  7  |NULL |\n" +
                "NULL |NULL |NULL |NULL |");
        // different two outer tables, left join
        sql = "select * from A left join D on a1=d1 where NOT exists (select 1 from C join B on c1=b1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |D1 | D2  |\n" +
                "------------------\n" +
                " 0 | 0 | 0 |  0  |\n" +
                " 0 | 0 | 0 |  0  |\n" +
                " 7 |70 | 7 |NULL |");
    }

    @Test
    public void multipleOuterTables_withoutExplicitJoin() throws Exception {
        // two outer tables
        String sql = "select * from A, B where a1=b1 and NOT exists (select 1 from C where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 0 | 0 | 0 | 0 |\n" +
                " 0 | 0 | 0 | 0 |");
        // three outer tables
        sql = "select * from A, B, C where a1=b1 and c1=b1 and b2 > 20 and NOT exists (select 1 from D where a1=d1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |C1 |C2 |\n" +
                "------------------------\n" +
                " 4 |40 | 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 | 4 |40 |");
        // two outer tables, two subquery queries
        sql = "select * from A, B where a1=b1 and b2 > 20 and NOT exists (select 1 from C,D where c1=d1 and a1=d1)";
        assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 4 |40 | 4 |40 |\n" +
                " 4 |40 | 4 |40 |");
    }

    @Test
    public void deleteOverNotExists_uncorrelated() throws Exception {
        methodWatcher.executeUpdate("create table Y(y1 int)");
        methodWatcher.executeUpdate("create table Z(z1 int)");

        // uncorrelated -- basic case
        methodWatcher.executeUpdate("insert into Y values(1),(2),(3)");
        int deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z)");
        assertEquals(3, deleteCount);
        assertEquals(0L, methodWatcher.query("select count(*) from Y"));

        // uncorrelated -- all rows excluded because of not exists subquery
        methodWatcher.executeUpdate("insert into Y values(1),(2),(3)");
        methodWatcher.executeUpdate("insert into Z values(1)");
        deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z)");
        assertEquals(0, deleteCount);

        // uncorrelated -- not exists subquery does not exclude any rows because of a predicate it has
        deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z where z1 > 100)");
        assertEquals(3, deleteCount);
        assertEquals(0L, methodWatcher.query("select count(*) from Y"));
    }

    @Test
    public void deleteOverNotExists_correlated() throws Exception {
        methodWatcher.executeUpdate("create table YY(y1 int)");
        methodWatcher.executeUpdate("create table ZZ(z1 int)");

        // correlated -- basic case
        methodWatcher.executeUpdate("insert into YY values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
        methodWatcher.executeUpdate("insert into ZZ values(2),(4),(6),(8),(10)");
        int deleteCount = methodWatcher.executeUpdate("delete from YY where not exists (select 1 from ZZ where y1=z1)");
        assertEquals(5, deleteCount);
        // verify that only expected rows remain in target table
        assertEquals("[2, 4, 6, 8, 10]", methodWatcher.queryList("select y1 from YY").toString());
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // mixed EXISTS and NOT-EXISTS
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void mixedExistsAndNotExists() throws Exception {
        assertUnorderedResult(conn(), "" +
                "select a1 from A where " +
                "exists (select b1 from B where a1=b1 and b1 in (3,5))" +
                "and " +
                "NOT exists (select b1 from B where a1=b1 and b2 = 50)", ZERO_SUBQUERY_NODES, "" +
                "A1 |\n" +
                "----\n" +
                " 3 |"
        );
    }

    @Test
    public void mixedExistsAndNotExists_nested() throws Exception {
        assertUnorderedResult(conn(), "" +
                "select a1 from A where exists (select 1 from B where a1=b1 and NOT exists (select 1 from C where c1=b1))" +
                "", ZERO_SUBQUERY_NODES, "" +
                "A1 |\n" +
                "----\n" +
                " 0 |"
        );
        assertUnorderedResult(conn(), "" +
                "select a1 from A where NOT exists (select 1 from B where a1=b1 and exists (select 1 from C where c1=b1))" +
                "", ZERO_SUBQUERY_NODES, "" +
                "A1 |\n" +
                "----\n" +
                " 0 |\n" +
                " 7 |"
        );
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // UN-FLATTENED exists queries
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void notFlattened_or() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select b1 from B where a1=b1 and b1 in (3,5))" +
                        "or " +
                        "NOT exists (select b1 from B where a1=b1 and b1 in (0,3,5))", 2, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 0 |\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |\n" +
                        " 7 |"
        );
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select b1 from B where a1=b1 and b1 < 3) or a1=5", 1, "" +
                        "A1 |\n" +
                        "----\n" +
                        " 3 |\n" +
                        " 4 |\n" +
                        " 5 |\n" +
                        " 7 |"
        );

    }

    @Test
    public void notFlattened_havingSubquery() throws Exception {
        assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having sum(b1) > 0 and NOT exists(select 1 from C where c1 > 30)", 1, "" +
                        "B1 | 2  |\n" +
                        "---------\n" +
                        " 2 |40  |\n" +
                        " 3 |60  |\n" +
                        " 4 |80  |\n" +
                        " 5 |100 |"
        );
        assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having NOT exists(select 1 from C where c1 > 30)", 1, "" +
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
        assertUnorderedResult(conn(),
                "select A.* from A where " +
                        "NOT exists(select 1 from B where a1=b1 and " +
                        "NOT exists(select 1 from C where c1=a1))", 2, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |\n" +
                        " 7 |70 |"
        );

    }

    @Test
    public void notFlattened_correlatedWithOffset() throws Exception {
        /* I don't currently assert the result here because splice returns the wrong result: DB-4020 */
        // offset in subquery -- return rows in A that have more than one row in D where a1=d1;
        assertSubqueryNodeCount(conn(), "select * from A where NOT exists (select 1 from D where d1=a1 offset 1 rows)", ONE_SUBQUERY_NODE);
        // offset in subquery -- return rows in A that have more than two rows in D where a1=d1;
        assertSubqueryNodeCount(conn(), "select * from A where NOT exists (select 1 from D where d1=a1 offset 3 rows)", ONE_SUBQUERY_NODE);
    }

    @Test
    public void notFlattened_unCorrelatedWithOffset() throws Exception {
        // subquery with offset that eliminates all rows
        assertUnorderedResult(conn(), "select count(*) from A where NOT exists (select 1 from D offset 10000 rows)", ONE_SUBQUERY_NODE, "" +
                "1 |\n" +
                "----\n" +
                " 7 |"
        );
        // subquery with offset that DOES NOT eliminate all rows
        assertUnorderedResult(conn(), "select count(*) from A where NOT exists (select 1 from D offset 1 rows)", ONE_SUBQUERY_NODE, "" +
                "1 |\n" +
                "----\n" +
                " 0 |"
        );
    }

    @Test
    public void notFlattened_correlatedWithLimits() throws Exception {
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from D where d1=a1 {limit 1})", ONE_SUBQUERY_NODE, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 2 |20 |\n" +
                " 4 |40 |"
        );
    }

    @Test
    public void notFlattened_unCorrelatedWithLimits() throws Exception {
        assertUnorderedResult(conn(), "select count(*) from A where exists (select 1 from D {limit 1})", ONE_SUBQUERY_NODE, "" +
                "1 |\n" +
                "----\n" +
                " 7 |"
        );
    }


    private TestConnection conn() {
        return methodWatcher.getOrCreateConnection();
    }
}
