package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;

import static com.splicemachine.subquery.SubqueryITUtil.*;

/**
 * Test EXIST subquery flattening for subqueries with unions.
 */
public class Subquery_Flattening_Exists_Union_IT {

    private static final String SCHEMA = Subquery_Flattening_Exists_Union_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/SubqueryFlatteningTestTables.sql", "");
    }

    @Test
    public void union_unCorrelated() throws Exception {
        // union of empty tables
        String sql = "select * from A where exists(select 1 from EMPTY_TABLE union select 1 from EMPTY_TABLE)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // union one non-empty first subquery
        sql = "select * from A where exists(select 1 from C union select 1 from EMPTY_TABLE)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // union one non-empty second subquery
        sql = "select * from A where exists(select 1 from EMPTY_TABLE union select 1 from C )";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // union, three unions
        sql = "select * from A where exists(select 1 from EMPTY_TABLE union select 1 from C union select 1 from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // non-empty tables, where subquery predicates eliminate all rows
        sql = "select * from A where exists(select 1 from C where c1 > 999999 union select 1 from D where d1 < -999999)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // non-empty tables, where subquery predicates eliminate all rows in ONE table
        sql = "select * from A where exists(select 1 from C where c1 > 999999 union select 1 from D where d1 = 0)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // unions with column references
        sql = "select * from A where exists(select c1,c2 from C union select d1,d2 from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // unions referencing all columns
        sql = "select * from A where exists(select * from C union select * from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // union same table
        sql = "select * from A where exists(select * from A union select * from A)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);
    }

    @Test
    public void union_unCorrelated_largeUnionCount() throws Exception {
        // union of empty tables
        String sql = "select * from A where exists(" +
                "      select e1 from EMPTY_TABLE " +
                "union select e2 from EMPTY_TABLE " +
                "union select e3 from EMPTY_TABLE " +
                "union select e4 from EMPTY_TABLE " +
                "union select 10 from EMPTY_TABLE " +
                "union select e1 from EMPTY_TABLE " +
                "union select e2 from EMPTY_TABLE " +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // Important performance optimization from DB-4081, convert all but top 'union' to 'union all' in exists/not-exist subquery.
        assertDistinctNodeCount(conn(), sql, 1);

        // union of non-empty tables
        sql = "select * from A where exists(" +
                "      select e1 from EMPTY_TABLE " +
                "union select a1 from A " +
                "union select a2 from A " +
                "union select b1 from B " +
                "union select c1 from C " +
                "union select d1 from D " +
                "union select e2 from EMPTY_TABLE " +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // Important performance optimization from DB-4081, convert all but top 'union' to 'union all' in exists/not-exist subquery.
        assertDistinctNodeCount(conn(), sql, 1);
    }

    @Test
    public void union_correlated() throws Exception {
        // union of empty tables
        String sql = "select * from A where exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from EMPTY_TABLE e where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // union of empty tables - each subquery selects different columns
        sql = "select * from A where exists(select e1 from EMPTY_TABLE where e1=a1 union select e2 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // union of empty tables - each subquery selects multiple different columns
        sql = "select * from A where exists(select e1,e2 from EMPTY_TABLE where e1=a1 union select e3,e4 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // union one non-empty first subquery
        sql = "select * from A where exists(select 1 from C where c1=a1 union select 1 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                " 2 |20  |\n" +
                " 5 |50  |");

        // union one non-empty second subquery
        sql = "select * from A where exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from C where c1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                " 2 |20  |\n" +
                " 5 |50  |");

        // union no non-empty
        sql = "select * from A where exists(select 1 from D where d1=a1 union select 1 from C where c1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");
    }

    @Test
    public void union_correlated_largeUnionCount() throws Exception {
        // union of non-empty tables
        String sql = "select * from A where exists(" +
                "      select e1 from EMPTY_TABLE where e1=a1 " +
                "union select b1 from B where b1=a1 " +
                "union select b2 from B where b2=a1 " +
                "union select c1 from C where c1=a1 " +
                "union select c2 from C where c2=a1 " +
                "union select d1 from D where d1=a1 " +
                "union select d2 from D where d2=a1 " +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 3 |30  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");

        // Important performance optimization from DB-4081, convert all but top 'union' to 'union all' in exists/not-exist subquery.
        assertDistinctNodeCount(conn(), sql, 1);
    }

    @Test
    public void union_correlated_lotsOfSubqueryPredicates() throws Exception {
        // union no non-empty
        String sql = "select * from A where exists(" + "" +
                "select 1 from D where d1=a1 and d1!=11 and d1 not in (12,13) and d2 < 70" +
                "union " +
                "select 1 from C where c1=a1 and c1!=11 and c2!=20" +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 5 |50 |\n" +
                " 6 |60 |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // UNION ALL
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    @Test
    public void unionAll_unCorrelated() throws Exception {
        String sql = "select * from A where exists(select 1 from EMPTY_TABLE union ALL select 1 from EMPTY_TABLE)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

        // non-empty tables, where subquery predicates eliminate all rows in ONE table
        sql = "select * from A where exists(select 1 from C where c1 > 999999 union ALL select 1 from D where d1 = 0)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);

        // non-empty tables, where union all subqueries return duplicate rows
        sql = "select * from A where exists(select 1 from C union ALL select 1 from B union ALL select 1 from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, RESULT_ALL_OF_A);
    }

    @Test
    public void unionAll_correlated() throws Exception {
        String R = "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                " 2 |20  |\n" +
                " 5 |50  |";

        // union-all one non-empty first subquery
        String sql = "select * from A where exists(select 1 from C where c1=a1 union ALL select 1 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, R);

        // union-all one non-empty second subquery
        sql = "select * from A where exists(select 1 from EMPTY_TABLE where e1=a1 union ALL select 1 from C where c1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, R);

        // union-all large union count
        sql = "select * from A where exists(" +
                "          select b1 from B where b1=a1 " +
                "union ALL select c1 from C where c1=a1 "+
                "union ALL select c2 from C where c2=a1 "+
                "union ALL select d1 from D where d1=a1 "+
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 3 |30  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // misc
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void union_unionsInFromListOfExistsSubquery() throws Exception {
        String sql = "select * from A where exists(" +
                "select * from (select c1 r from C union select d1 r from D) foo where foo.r=a1" +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");
        // Important performance optimization from DB-4081, convert all but top 'union' to 'union all' in exists/not-exist subquery.
        assertDistinctNodeCount(conn(), sql, 1);

        // analogous to the query in DB-4081
        sql = "select * from A where exists(" +
                "select * from (select b1 r from B union select c1 r from C union select d1 r from D) foo where foo.r=a1" +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 3 |30  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");
        // Important performance optimization from DB-4081, convert all but top 'union' to 'union all' in exists/not-exist subquery.
        assertDistinctNodeCount(conn(), sql, 1);
    }

    @Test
    public void mixedUnionAndUnionAll() throws Exception {
        // union-all one non-empty first subquery
        String sql = "select * from A where exists(" +
                "          select 1 from B where a1=b1 " +
                "union ALL select 1 from C where a1=c1 " +
                "union     select 1 from D where a1=d1 " +
                "union     select 1 from EMPTY_TABLE where a1=e1 " +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                " 2 |20  |\n" +
                " 3 |30  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // not flattened
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void notFlattened_unionsReferenceMultipleOuterTableColumns() throws Exception {
        // same table in each union select
        String sql = "select * from A where exists(select 1 from D where d1=a1 union select 1 from D where d2=a2)";
        assertUnorderedResult(conn(), sql, ONE_SUBQUERY_NODE, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "12 |120 |\n" +
                "12 |120 |\n" +
                "13 | 0  |\n" +
                " 5 |50  |\n" +
                " 6 |60  |\n" +
                " 7 |70  |");

        // different table in each union select
        sql = "select * from A where exists(select 1 from C where c1=a1 union select 1 from D where d1=a2)";
        assertUnorderedResult(conn(), sql, ONE_SUBQUERY_NODE, "" +
                "A1 |A2  |\n" +
                "---------\n" +
                " 0 | 0  |\n" +
                " 1 |10  |\n" +
                "11 |110 |\n" +
                "13 | 0  |\n" +
                "13 | 1  |\n" +
                " 2 |20  |\n" +
                " 5 |50  |");
    }

    private Connection conn() {
        return methodWatcher.getOrCreateConnection();
    }

}