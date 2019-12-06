/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import org.junit.*;

import java.sql.ResultSet;

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
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/SubqueryFlatteningTestTables.sql", "");
    }

    @Test
    public void uncorrelated_oneSubqueryTable() throws Exception {

        // subquery reads same table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select a1 from A ai where ai.a2 > 20)", ONE_SUBQUERY_NODE, "");

        // subquery reads different table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select b1 from B where b2 > 20)", ONE_SUBQUERY_NODE, "");

        // empty table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from EMPTY_TABLE)", ONE_SUBQUERY_NODE, RESULT_ALL_OF_A);

        // two exists
        assertUnorderedResult(conn(),
                "select * from A where " +
                        "NOT exists (select b1 from B where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B where b1 > 3)", TWO_SUBQUERY_NODES, "");

        // two exists, different rows
        assertUnorderedResult(conn(),
                "select * from A where " +
                        "NOT exists (select b1 from B where b2 < 0)" +
                        " and " +
                        "NOT exists (select b1 from B where b1 < 0)", TWO_SUBQUERY_NODES, RESULT_ALL_OF_A);
    }


    @Test
    public void uncorrelated_twoSubqueryTables() throws Exception {
        // subquery reads same table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select a1 from A ai join D on a1=d1 where ai.a2 > 20)", ONE_SUBQUERY_NODE, "");

        // subquery reads different table
        assertUnorderedResult(conn(), "select * from A where NOT exists (select b1 from B join D on b1=d1 where b2 > 20)", ONE_SUBQUERY_NODE, "");

        // two NOT-exists, both return rows
        assertUnorderedResult(conn(),
                "select * from A where " +
                        "NOT exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B join D on b1=d1 where b1 > 3)", TWO_SUBQUERY_NODES, "");

        // two NOT-exists, one of which excludes all rows
        assertUnorderedResult(conn(),
                "select * from A where " +
                        "NOT exists (select b1 from B join D on b1=d1 where b2 > 20)" +
                        " and " +
                        "NOT exists (select b1 from B join D on b1=d1 where b1 < 0)", TWO_SUBQUERY_NODES, "");

        // two NOT-exists, both of which excludes all rows
        assertUnorderedResult(conn(),
                "select * from A where " +
                        "NOT exists (select b1 from B join D on b1=d1 where b2 < 0)" +
                        " and " +
                        "NOT exists (select b1 from B join D on b1=d1 where b1 < 0)", TWO_SUBQUERY_NODES, RESULT_ALL_OF_A);
    }

    @Test
    public void uncorrelated_threeSubqueryTables() throws Exception {
        assertUnorderedResult(conn(),
                "select A.* from A where NOT exists (select 1 from A ai join B on ai.a1=b1 join C on b1=c1 join D on c1=d1)", ONE_SUBQUERY_NODE, "");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // correlated
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void correlated_oneSubqueryTable() throws Exception {
        String R = "" +
                "A1  |\n" +
                "------\n" +
                " 12  |\n" +
                " 12  |\n" +
                " 13  |\n" +
                " 13  |\n" +
                "  4  |\n" +
                "  5  |\n" +
                "  7  |\n" +
                "NULL |";

        /* subquery selects constant */
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where a1 = b1)", ZERO_SUBQUERY_NODES, R);
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where b1 = a1)", ZERO_SUBQUERY_NODES, R);

        /* subquery selects b1 */
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select b1 from B where a1 = b1)", ZERO_SUBQUERY_NODES, R);

        /* subquery selects b1 multiple times */
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select b1,b1,b1 from B where a1 = b1)", ZERO_SUBQUERY_NODES, R);

        /* subquery selects constant multiple times */
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1,2,3 from B where a1 = b1)", ZERO_SUBQUERY_NODES, R);

        /* subquery selects all */
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select * from B where a1 = b1)", ZERO_SUBQUERY_NODES, R);

        /* not exists B join C */
        assertUnorderedResult(conn(),
                "select * from B where NOT exists (select * from C where b1 = c1)", ZERO_SUBQUERY_NODES, "" +
                        "B1  | B2  |\n" +
                        "------------\n" +
                        "  3  | 30  |\n" +
                        "  6  | 60  |\n" +
                        "  9  | 90  |\n" +
                        "NULL |NULL |\n" +
                        "NULL |NULL |"
        );
        /* not exists C join D */
        assertUnorderedResult(conn(),
                "select * from C where NOT exists (select * from D where d1 = c1)", ZERO_SUBQUERY_NODES, "" +
                        "C1  | C2  |\n" +
                        "------------\n" +
                        " 10  | 100 |\n" +
                        "  2  | 20  |\n" +
                        "  8  | 80  |\n" +
                        "NULL |NULL |\n" +
                        "NULL |NULL |\n" +
                        "NULL |NULL |"
        );
    }

    @Test
    public void correlated_twoSubqueryTables() throws Exception {
        /* no extra predicates */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1=b1)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  5  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
        /* restriction on C */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1=b1 and b1!=0)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        "  0  |\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  5  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
        /* restriction on B and C */
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select 1 from B join C on b1=c1 where a1=b1 and b1!=0 and c1!=2)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        "  0  |\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  2  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  5  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
    }

    @Test
    public void correlated_threeSubqueryTables() throws Exception {
        String R = "A1  |\n" +
                "------\n" +
                " 12  |\n" +
                " 12  |\n" +
                " 13  |\n" +
                " 13  |\n" +
                "  2  |\n" +
                "  3  |\n" +
                "  4  |\n" +
                "  5  |\n" +
                "  6  |\n" +
                "  7  |\n" +
                "NULL |";

        // A join B join C join D
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B join C on b1=c1 join D on c1=d1 where a1=b1)", ZERO_SUBQUERY_NODES, R);
        // A join D join C join B
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from D join C on d1=c1 join B on c1=b1 where a1=b1)", ZERO_SUBQUERY_NODES, R);

        // D join A join B join C
        assertUnorderedResult(conn(), "select d1 from D where NOT exists (select 1 from A join B on a1=b1 join C on c1=b1 where a1=d1)", ZERO_SUBQUERY_NODES, "" +
                "D1  |\n" +
                "------\n" +
                " 12  |\n" +
                " 12  |\n" +
                "  5  |\n" +
                "  6  |\n" +
                "  7  |\n" +
                "NULL |\n" +
                "NULL |\n" +
                "NULL |\n" +
                "NULL |");
    }

    @Test
    public void correlated_withMultipleCorrelationPredicates() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where not exists (select 1 from D where a1=d1 and a2=d2)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  2  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "NULL |");
    }

    /* We do flatten these for EXISTS. But not for NOT EXISTS.  See notes on why in ExistsSubqueryWhereVisitor.java*/
    @Test
    public void correlated_withCorrelatedColumnRefComparedToConstant() throws Exception {
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where a1=3)", 1, "" +
                "A1  |\n" +
                "------\n" +
                "  0  |\n" +
                "  1  |\n" +
                " 11  |\n" +
                " 12  |\n" +
                " 12  |\n" +
                " 13  |\n" +
                " 13  |\n" +
                "  2  |\n" +
                "  4  |\n" +
                "  5  |\n" +
                "  6  |\n" +
                "  7  |\n" +
                "NULL |"
        );
        assertUnorderedResult(conn(), "select a1 from A where NOT exists (select 1 from B where a1=6 and a1=b1)", 1, "" +
                "A1  |\n" +
                "------\n" +
                "  0  |\n" +
                "  1  |\n" +
                " 11  |\n" +
                " 12  |\n" +
                " 12  |\n" +
                " 13  |\n" +
                " 13  |\n" +
                "  2  |\n" +
                "  3  |\n" +
                "  4  |\n" +
                "  5  |\n" +
                "  7  |\n" +
                "NULL |");
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
                        "NOT exists (select 1 from B where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  4  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
        // one tables in 3 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1)" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1 and d1 in (3,5,12,13))", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  4  |\n" +
                        "  7  |\n" +
                        "NULL |");
    }

    @Test
    public void multipleExistsSubqueries_twoTablesPerSubquery() throws Exception {
        // one tables in 2 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B join C on b1=c1 where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "NULL |"
        );
        // one tables in 3 subqueries
        assertUnorderedResult(conn(),
                "select a1 from A where " +
                        "NOT exists (select 1 from B where a1=b1)" +
                        " and " +
                        "NOT exists (select 1 from C where a1=c1)" +
                        " and " +
                        "NOT exists (select 1 from D where a1=d1 and d1 in (3,5,7))", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  4  |\n" +
                        "NULL |");
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
                        "A1  |\n" +
                        "------\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  2  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
    }

    @Test
    public void correlated_leftJoinInSubquery() throws Exception {
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select * from C left join D on c1=d1 where a1=c1)", ZERO_SUBQUERY_NODES, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
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
                " 3 |30 | 3 |30 |");
    }

    @Test
    public void outerSelectIsLeftJoinNode() throws Exception {
        String sql = "select * from A left join (select * from B where b1 > 0 and NOT exists (select 1 from D where d1=b1)) AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  | B1  | B2  |\n" +
                "------------------------\n" +
                "  0  |  0  |NULL |NULL |\n" +
                "  1  | 10  |NULL |NULL |\n" +
                " 11  | 110 |NULL |NULL |\n" +
                " 12  | 120 |NULL |NULL |\n" +
                " 12  | 120 |NULL |NULL |\n" +
                " 13  |  0  |NULL |NULL |\n" +
                " 13  |  1  |NULL |NULL |\n" +
                "  2  | 20  |  2  | 20  |\n" +
                "  3  | 30  |  3  | 30  |\n" +
                "  4  | 40  |NULL |NULL |\n" +
                "  5  | 50  |NULL |NULL |\n" +
                "  6  | 60  |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |");
    }

    @Test
    public void outerSelectIsRightJoinNode() throws Exception {
        String sql = "select * from A right join (select * from B where b1 > 3 and NOT exists (select 1 from D where d1=b1)) AS foo on a1=foo.b1";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  |B1 |B2 |\n" +
                "--------------------\n" +
                "NULL |NULL | 8 |80 |\n" +
                "NULL |NULL | 9 |90 |");
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
                "A1  | A2  | B1  |colAlias |\n" +
                "----------------------------\n" +
                "  0  |  0  |NULL |   NN    |\n" +
                "  1  | 10  |NULL |   NN    |\n" +
                " 11  | 110 |NULL |   NN    |\n" +
                " 12  | 120 |NULL |   NN    |\n" +
                " 12  | 120 |NULL |   NN    |\n" +
                " 13  |  0  |NULL |   NN    |\n" +
                " 13  |  1  |NULL |   NN    |\n" +
                "  2  | 20  |  2  |   YY    |\n" +
                "  3  | 30  |NULL |   NN    |\n" +
                "  4  | 40  |NULL |   NN    |\n" +
                "  5  | 50  |NULL |   NN    |\n" +
                "  6  | 60  |NULL |   NN    |\n" +
                "  7  | 70  |NULL |   NN    |\n" +
                "NULL |NULL |NULL |   NN    |");
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
                "A1  | A2  |\n" +
                "------------\n" +
                "  0  |  0  |\n" +
                "  1  | 10  |\n" +
                " 11  | 110 |\n" +
                " 12  | 120 |\n" +
                " 12  | 120 |\n" +
                " 13  |  0  |\n" +
                " 13  |  1  |\n" +
                "  2  | 20  |\n" +
                "  4  | 40  |\n" +
                "  5  | 50  |\n" +
                "  7  | 70  |\n" +
                "NULL |NULL |"
        );
    }

    @Test
    public void nestedExists_twoLevels() throws Exception {
        String sql = "select A.* from A where NOT exists(" +
                "select 1 from B where a1=b1 and NOT exists(" +
                "select 1 from C where b1=c1 and NOT exists(" +
                "select 1 from D where d1=c1" + ")))";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  |\n" +
                "------------\n" +
                " 12  | 120 |\n" +
                " 12  | 120 |\n" +
                " 13  |  0  |\n" +
                " 13  |  1  |\n" +
                "  2  | 20  |\n" +
                "  4  | 40  |\n" +
                "  5  | 50  |\n" +
                "  7  | 70  |\n" +
                "NULL |NULL |");

        sql = "select A.* from A where NOT exists(" +
                "select 1 from B where a1=b1 and b1!=11 and NOT exists(" +
                "select 1 from C where b1=c1 and c1!=11 and NOT exists(" +
                "select 1 from D where d1=c1 and d1!=11" + ")))";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  |\n" +
                "------------\n" +
                " 11  | 110 |\n" +
                " 12  | 120 |\n" +
                " 12  | 120 |\n" +
                " 13  |  0  |\n" +
                " 13  |  1  |\n" +
                "  2  | 20  |\n" +
                "  4  | 40  |\n" +
                "  5  | 50  |\n" +
                "  7  | 70  |\n" +
                "NULL |NULL |");
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
        assertUnorderedResult(conn(), "select a1 from AA where NOT exists(select 1 from BB join CC on b2=c1)", ONE_SUBQUERY_NODE, "");
    }

    @Test
    public void multipleOuterTables_withExplicitJoin() throws Exception {
        String sql = "select * from A join B on a1=b1 where NOT exists (select 1 from C where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 3 |30 | 3 |30 |\n" +
                " 6 |60 | 6 |60 |");

        // two subquery tables
        sql = "select * from A join B on a1=b1 where NOT exists (select 1 from C join D on c1=d1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 6 |60 | 6 |60 |");

        // different two outer tables, right join
        sql = "select * from A right join D on a1=d1 where NOT exists (select 1 from C join B on c1=b1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  | D1  | D2  |\n" +
                "------------------------\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                "  5  | 50  |  5  | 50  |\n" +
                "  6  | 60  |  6  | 60  |\n" +
                "  7  | 70  |  7  | 70  |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |");

        // different two outer tables, left join
        sql = "select * from A left join D on a1=d1 where NOT exists (select 1 from C join B on c1=b1 where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1  | A2  | D1  | D2  |\n" +
                "------------------------\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 12  | 120 | 12  | 120 |\n" +
                " 13  |  0  |NULL |NULL |\n" +
                " 13  |  1  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  4  | 40  |NULL |NULL |\n" +
                "  5  | 50  |  5  | 50  |\n" +
                "  6  | 60  |  6  | 60  |\n" +
                "  7  | 70  |  7  | 70  |\n" +
                "NULL |NULL |NULL |NULL |");
    }

    @Test
    public void multipleOuterTables_withoutExplicitJoin() throws Exception {
        // two outer tables
        String sql = "select * from A, B where a1=b1 and NOT exists (select 1 from C where a1=c1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 3 |30 | 3 |30 |\n" +
                " 6 |60 | 6 |60 |");

        // three outer tables
        sql = "select * from A, B, C where a1=b1 and c1=b1 and NOT exists (select 1 from D where a1=d1)";
        assertUnorderedResult(conn(), sql, ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |B1 |B2 |C1 |C2 |\n" +
                "------------------------\n" +
                " 2 |20 | 2 |20 | 2 |20 |");

        // two outer tables, two subquery queries
        sql = "select * from A, B where a1=b1 and b2 > 20 and NOT exists (select 1 from C,D where c1=d1 and a1=d1)";
        assertUnorderedResult(conn(), sql, 1, "" +
                "A1 |A2 |B1 |B2 |\n" +
                "----------------\n" +
                " 3 |30 | 3 |30 |\n" +
                " 6 |60 | 6 |60 |");
    }

    @Test
    public void deleteOverNotExists_uncorrelated() throws Exception {
        methodWatcher.executeUpdate("create table Y(y1 int)");
        methodWatcher.executeUpdate("create table Z(z1 int)");

        // uncorrelated -- basic case
        methodWatcher.executeUpdate("insert into Y values(1),(2),(3)");
        int deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z)");
        assertEquals(3, deleteCount);
        assertEquals(0L, (long)methodWatcher.query("select count(*) from Y"));

        // uncorrelated -- all rows excluded because of not exists subquery
        methodWatcher.executeUpdate("insert into Y values(1),(2),(3)");
        methodWatcher.executeUpdate("insert into Z values(1)");
        deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z)");
        assertEquals(0, deleteCount);

        // uncorrelated -- not exists subquery does not exclude any rows because of a predicate it has
        deleteCount = methodWatcher.executeUpdate("delete from Y where not exists (select 1 from Z where z1 > 100)");
        assertEquals(3, deleteCount);
        assertEquals(0L, (long)methodWatcher.query("select count(*) from Y"));
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
        assertEquals("[2, 4, 6, 8, 10]", methodWatcher.queryList("select y1 from YY order by 1").toString());
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
                " 3 |\n" +
                " 6 |"
        );
        assertUnorderedResult(conn(), "" +
                "select a1 from A where NOT exists (select 1 from B where a1=b1 and exists (select 1 from C where c1=b1))" +
                "", ZERO_SUBQUERY_NODES, "" +
                "A1  |\n" +
                "------\n" +
                " 12  |\n" +
                " 12  |\n" +
                " 13  |\n" +
                " 13  |\n" +
                "  3  |\n" +
                "  4  |\n" +
                "  5  |\n" +
                "  6  |\n" +
                "  7  |\n" +
                "NULL |"
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
                        "A1  |\n" +
                        "------\n" +
                        "  0  |\n" +
                        "  1  |\n" +
                        " 11  |\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  2  |\n" +
                        "  4  |\n" +
                        "  5  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );
        assertUnorderedResult(conn(),
                "select a1 from A where NOT exists (select b1 from B where a1=b1 and b1 < 3) or a1=11", ONE_SUBQUERY_NODE, "" +
                        "A1  |\n" +
                        "------\n" +
                        " 11  |\n" +
                        " 12  |\n" +
                        " 12  |\n" +
                        " 13  |\n" +
                        " 13  |\n" +
                        "  3  |\n" +
                        "  4  |\n" +
                        "  5  |\n" +
                        "  6  |\n" +
                        "  7  |\n" +
                        "NULL |"
        );

    }

    @Test
    public void notFlattened_havingSubquery() throws Exception {
        assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having sum(b1) > 0 and NOT exists(select 1 from C where c1 > 30)", ONE_SUBQUERY_NODE, "" +
                        "B1 | 2  |\n" +
                        "---------\n" +
                        "11 |220 |\n" +
                        " 2 |20  |\n" +
                        " 3 |30  |\n" +
                        " 6 |60  |\n" +
                        " 8 |80  |\n" +
                        " 9 |90  |"
        );
        assertUnorderedResult(conn(),
                "select b1, sum(b2) " +
                        "from B " +
                        "where b1 > 1 " +
                        "group by b1 " +
                        "having NOT exists(select 1 from C where c1 > 30)", ONE_SUBQUERY_NODE, "" +
                        "B1 | 2  |\n" +
                        "---------\n" +
                        "11 |220 |\n" +
                        " 2 |20  |\n" +
                        " 3 |30  |\n" +
                        " 6 |60  |\n" +
                        " 8 |80  |\n" +
                        " 9 |90  |"
        );
    }

    @Test
    public void notFlattened_multiLevelCorrelationPredicate() throws Exception {
        assertUnorderedResult(conn(),
                "select A.* from A where " +
                        "NOT exists(select 1 from B where a1=b1 and " +
                        "NOT exists(select 1 from C where c1=a1))", 2, "" +
                        "A1  | A2  |\n" +
                        "------------\n" +
                        "  0  |  0  |\n" +
                        "  1  | 10  |\n" +
                        " 11  | 110 |\n" +
                        " 12  | 120 |\n" +
                        " 12  | 120 |\n" +
                        " 13  |  0  |\n" +
                        " 13  |  1  |\n" +
                        "  2  | 20  |\n" +
                        "  4  | 40  |\n" +
                        "  5  | 50  |\n" +
                        "  7  | 70  |\n" +
                        "NULL |NULL |"
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
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from D offset 10000 rows)", ONE_SUBQUERY_NODE, RESULT_ALL_OF_A);
        // subquery with offset that DOES NOT eliminate all rows
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from D offset 1 rows)", ONE_SUBQUERY_NODE, "");
    }

    @Test
    public void notFlattened_correlatedWithLimits() throws Exception {
        assertUnorderedResult(conn(), "select * from A where NOT exists (select 1 from D where d1=a1 {limit 1})", ONE_SUBQUERY_NODE, "" +
                "A1  | A2  |\n" +
                "------------\n" +
                " 13  |  0  |\n" +
                " 13  |  1  |\n" +
                "  2  | 20  |\n" +
                "  3  | 30  |\n" +
                "  4  | 40  |\n" +
                "NULL |NULL |"
        );
    }

    @Test
    public void notFlattened_unCorrelatedWithLimits() throws Exception {
        assertUnorderedResult(conn(), "select * from A where exists (select 1 from D {limit 1})", ONE_SUBQUERY_NODE, RESULT_ALL_OF_A);
    }

    @Test
    public void testNonCorrelatedExistsPlan() throws Exception {
        /* predicate where expression on columns is not pushed down */
        String sqlText = "select * from A where not exists (select * from B where 1=0) and a1 < 2";

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=8,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=7,totalCost=8.059,outputRows=2,outputHeapSize=3 B,partitions=1)
            ->  ProjectRestrict(n=6,totalCost=4.04,outputRows=2,outputHeapSize=3 B,partitions=1,preds=[is null(subq=4)])
              ->  Subquery(n=5,totalCost=0.002,outputRows=1,outputHeapSize=0 B,partitions=1,correlated=false,expression=true,invariant=true)
                ->  Limit(n=4,totalCost=0.002,outputRows=1,outputHeapSize=0 B,partitions=1,fetchFirst=1)
                  ->  ProjectRestrict(n=3,totalCost=0.002,outputRows=1,outputHeapSize=0 B,partitions=1)
                    ->  Values(n=2,totalCost=0.002,outputRows=1,outputHeapSize=0 B,partitions=1)
              ->  TableScan[A(2160)](n=1,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=3 B,partitions=1,preds=[(A1[0:1] < 2)])

        8 rows selected
         */
        SpliceUnitTest.rowContainsQuery(new int[]{3,4,5,7}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[is null(subq=4)])"},
                new String[]{"Subquery", "correlated=false,expression=true,invariant=true"},
                new String[]{"Limit", "fetchFirst=1"},
                new String[]{"Values"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNonCorrelatedExistsPlanWithSetOperation() throws Exception {
        /* predicate where expression on columns is not pushed down */
        String sqlText = "select * from A where not exists (select b1 from B union all select c1 from C) and a1 < 2";

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=10,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=8.059,outputRows=2,outputHeapSize=3 B,partitions=1)
            ->  ProjectRestrict(n=8,totalCost=4.04,outputRows=2,outputHeapSize=3 B,partitions=1,preds=[is null(subq=9)])
              ->  Subquery(n=7,totalCost=37.292,outputRows=1,outputHeapSize=40 B,partitions=1,correlated=false,expression=true,invariant=true)
                ->  Limit(n=6,totalCost=37.24,outputRows=1,outputHeapSize=40 B,partitions=1,fetchFirst=1)
                  ->  ProjectRestrict(n=5,totalCost=37.24,outputRows=1,outputHeapSize=40 B,partitions=1)
                    ->  Union(n=4,totalCost=37.24,outputRows=1,outputHeapSize=40 B,partitions=1)
                      ->  TableScan[C(2192)](n=3,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=40 B,partitions=1)
                      ->  TableScan[B(2176)](n=2,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=40 B,partitions=1)
              ->  TableScan[A(2160)](n=1,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=3 B,partitions=1,preds=[(A1[0:1] < 2)])
        10 rows selected
         */
        SpliceUnitTest.rowContainsQuery(new int[]{3, 4, 5, 7}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[is null(subq=9)]"},
                new String[]{"Subquery", "correlated=false,expression=true,invariant=true"},
                new String[]{"Limit", "fetchFirst=1"},
                new String[] {"Union"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNonCorrelatedExistsPlanWithValuesStatement() throws Exception {
        /* predicate where expression on columns is not pushed down */
        String sqlText = "select * from A where not exists (values 2) and a1 < 2";

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=6,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=5,totalCost=8.059,outputRows=2,outputHeapSize=3 B,partitions=1)
            ->  ProjectRestrict(n=4,totalCost=4.04,outputRows=2,outputHeapSize=3 B,partitions=1,preds=[is null(subq=1)])
              ->  Subquery(n=3,totalCost=0.001,outputRows=2,outputHeapSize=1 B,partitions=1,correlated=false,expression=true,invariant=true)
                ->  Values(n=2,totalCost=0.001,outputRows=2,outputHeapSize=1 B,partitions=1)
              ->  TableScan[A(2160)](n=1,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=3 B,partitions=1,preds=[(A1[0:1] < 2)])
        6 rows selected

         */
        SpliceUnitTest.rowContainsQuery(new int[]{3, 4, 5}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[is null(subq=0)]"},
                new String[]{"Subquery", "correlated=false,expression=true,invariant=true"},
                new String[] {"Values"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNonCorrelatedExistsPlanWithOffset() throws Exception {
        /* predicate where expression on columns is not pushed down */
        String sqlText = "select * from A where not exists (select * from B order by b2 offset 20 rows fetch next 10 rows ONLY) and a1 < 2";

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=8,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=7,totalCost=8.059,outputRows=2,outputHeapSize=3 B,partitions=1)
            ->  ProjectRestrict(n=6,totalCost=4.04,outputRows=2,outputHeapSize=3 B,partitions=1,preds=[is null(subq=4)])
              ->  Subquery(n=5,totalCost=0.521,outputRows=1,outputHeapSize=1 B,partitions=1,correlated=false,expression=true,invariant=true)
                ->  Limit(n=4,totalCost=0.416,outputRows=1,outputHeapSize=1 B,partitions=1,offset=20,fetchFirst=1)
                  ->  ProjectRestrict(n=3,totalCost=0.416,outputRows=1,outputHeapSize=1 B,partitions=1)
                    ->  TableScan[B(2176)](n=2,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=1 B,partitions=1)
              ->  TableScan[A(2160)](n=1,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=3 B,partitions=1,preds=[(A1[0:1] < 2)])
        8 rows selected
         */
        SpliceUnitTest.rowContainsQuery(new int[]{3, 4, 5}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[is null(subq=4)]"},
                new String[]{"Subquery", "correlated=false,expression=true,invariant=true"},
                new String[]{"Limit", "offset=20,fetchFirst=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    private TestConnection conn() {
        return methodWatcher.getOrCreateConnection();
    }
}
