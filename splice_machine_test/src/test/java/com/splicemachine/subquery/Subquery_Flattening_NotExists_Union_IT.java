package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;

import static com.splicemachine.subquery.SubqueryITUtil.*;

/**
 * Test NOT-EXIST subquery flattening for subqueries with unions.
 */
public class Subquery_Flattening_NotExists_Union_IT {

    private static final String SCHEMA = Subquery_Flattening_NotExists_Union_IT.class.getSimpleName();

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
    public void union_unCorrelated() throws Exception {
        // union of empty tables
        String sql = "select * from A where NOT exists(select 1 from EMPTY_TABLE union select 1 from EMPTY_TABLE)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |\n" +
                " 7 |70 |");

        // union one non-empty first subquery
        sql = "select count(*) from A where NOT exists(select 1 from C union select 1 from EMPTY_TABLE)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // union one non-empty second subquery
        sql = "select count(*) from A where NOT exists(select 1 from EMPTY_TABLE union select 1 from C)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // union, three unions
        sql = "select count(*) from A where NOT exists(select 1 from EMPTY_TABLE union select 1 from C union select 1 from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // non-empty tables, where subquery predicates eliminate all rows
        sql = "select count(*) from A where NOT exists(select 1 from C where c1 > 999999 union select 1 from D where d1 < -999999)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 7 |");

        // non-empty tables, where subquery predicates eliminate all rows in ONE table
        sql = "select count(*) from A where NOT exists(select 1 from C where c1 > 999999 union select 1 from D where d1 = 0)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");


        // unions with column references
        sql = "select count(*) from A where NOT exists(select c1,c2 from C union select d1,d2 from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // unions referencing all columns
        sql = "select count(*) from A where NOT exists(select * from C union select * from D)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");

        // union same table
        sql = "select count(*) from A where NOT exists(select * from A union select * from A)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 0 |");
    }

    @Test
    public void union_correlated() throws Exception {
        // union of empty tables
        String sql = "select count(*) from A where NOT exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 7 |");
        // union of empty tables - each subquery selects different columns
        sql = "select count(*) from A where NOT exists(select e1 from EMPTY_TABLE where e1=a1 union select e2 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 7 |");
        // union of empty tables - each subquery selects multiple different columns
        sql = "select count(*) from A where NOT exists(select e1,e2 from EMPTY_TABLE where e1=a1 union select e3,e4 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "1 |\n" +
                "----\n" +
                " 7 |");

        // union one non-empty first subquery
        sql = "select * from A where NOT exists(select 1 from C where c1=a1 union select 1 from EMPTY_TABLE where e1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 7 |70 |");

        // union one non-empty second subquery
        sql = "select * from A where NOT exists(select 1 from EMPTY_TABLE where e1=a1 union select 1 from C where c1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 7 |70 |");

        // union no non-empty
        sql = "select * from A where NOT exists(select 1 from D where d1=a1 union select 1 from C where c1=a1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "");

    }

    @Test
    public void union_unionsInFromListOfNotExistsSubquery() throws Exception {
        String sql = "select * from B where NOT exists(" +
                "select * from (select c1 r from C union select d1 r from D) foo where foo.r=b1" +
                ")";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "B1  | B2  |\n" +
                "------------\n" +
                "NULL |NULL |");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // not flattened
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void notFlattened_unionsReferenceMultipleOuterTableColumns() throws Exception {
        // same table in each union select
        String sql = "select * from A where NOT exists(select 1 from D where d1=a1 union select 1 from D where d2=a2)";
        assertUnorderedResult(conn(), sql, ONE_SUBQUERY_NODE, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 2 |20 |\n" +
                " 4 |40 |");

        // different table in each union select
        sql = "select * from A where NOT exists(select 1 from C where c1=a1 union select 1 from D where d1=a2)";
        assertUnorderedResult(conn(), sql, ONE_SUBQUERY_NODE, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 7 |70 |");
    }


    private Connection conn() {
        return methodWatcher.getOrCreateConnection();
    }


}
