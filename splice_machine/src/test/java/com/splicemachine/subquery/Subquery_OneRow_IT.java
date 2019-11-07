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

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_dao.SchemaDAO;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Statement;

import static com.splicemachine.subquery.SubqueryITUtil.ONE_SUBQUERY_NODE;
import static com.splicemachine.subquery.SubqueryITUtil.ZERO_SUBQUERY_NODES;

/**
 * Created by yxia on 10/22/19.
 */
public class Subquery_OneRow_IT extends SpliceUnitTest {

    private static final String SCHEMA = Subquery_OneRow_IT.class.getSimpleName();

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
            s.executeUpdate("create table t1 (a1 int, b1 int, c1 int, primary key (a1))");
            s.executeUpdate("create unique index idx_t1 on t1(c1,b1)");
            s.executeUpdate("insert into t1 values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10)");

            s.executeUpdate("create table t2 (a2 int, b2 int, c2 int, primary key (a2))");
            s.executeUpdate("create unique index idx_t2 on t2(c2,b2)");
            s.executeUpdate("insert into t2 values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10)");

            s.executeUpdate("create table t3 (a3 int, b3 int, c3 int, primary key (a3))");
            s.executeUpdate("create unique index idx_t3 on t3(c3,b3)");
            s.executeUpdate("insert into t3 values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10)");

            s.execute("analyze schema " + SCHEMA);
        }
    }

    @Test
    public void testInSubqueryWithSingleTableAndPK() throws Exception {
        /* one row determined through PK */
        String sql = "select * from t1 where a1 in (select b2 from t2 where a2=1)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |");
    }

    @Test
    public void testExpressionSubqueryWithSingleTableAndPK() throws Exception {
        /* one row determined through PK */
        String sql = "select * from t1 where a1=(select b2 from t2 where a2=1)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |");
    }

    @Test
    public void testANYSubqueryWithSingleTableAndPK() throws Exception {
        /* one row determined through PK */
        String sql = "select * from t1 where a1>ANY(select b2 from t2 where a2=8)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE, "A1 |B1 |C1 |\n" +
                "------------\n" +
                "10 |10 |10 |\n" +
                " 9 | 9 | 9 |");
    }

    @Test
    public void testANYSubqueryWithSingleTableAndNotPK() throws Exception {
        /* negative test case */
        String sql = "select * from t1 where a1 in (select b2 from t2 where b2=1)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 | 1 | 1 |");
    }

    @Test
    public void testInSubqueryWithSingleTableAndUI() throws Exception {
        /* one row determined through unique index */
        String sql = "select * from t1 where b1 in (select b2 from t2 where b2=1 and c2=1)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |");
    }

    @Test
    public void testExpressionSubqueryWithSingleTableAndUI() throws Exception {
        /* one row determined through unique index */
        String sql = "select * from t1 where b1 > (select b2 from t2 where b2=8 and c2=8)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        "10 |10 |10 |\n" +
                        " 9 | 9 | 9 |");
    }

    @Test
    public void testANYSubqueryWithSingleTableAndUI() throws Exception {
        /* one row determined through unique index */
        String sql = "select * from t1 where b1 =ANY (select b2 from t2 where b2=1 and c2=1)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |");
    }

    @Test
    public void testInSubqueryWithMutipleTables() throws Exception {
        String sql = "select * from t1 where a1 in (select a2 from t2, t3 where b2=1 and c2=c3 and c3=1 and b3=2)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "");
    }

    @Test
    public void testExpressionSubqueryWithMutipleTables() throws Exception {
        String sql = "select * from t1 where a1 = (select a2 from t2, t3 where b2=1 and c2=c3 and c3=1 and b3=1)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 | 1 | 1 |");
    }

    @Test
    public void testANYSubqueryWithMutipleTables() throws Exception {
        String sql = "select * from t1 where a1 <= ANY (select a2 from t2, t3 where b2=10 and c2=c3 and a3=10)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 | 1 | 1 |\n" +
                        "10 |10 |10 |\n" +
                        " 2 | 2 | 2 |\n" +
                        " 3 | 3 | 3 |\n" +
                        " 4 | 4 | 4 |\n" +
                        " 5 | 5 | 5 |\n" +
                        " 6 | 6 | 6 |\n" +
                        " 7 | 7 | 7 |\n" +
                        " 8 | 8 | 8 |\n" +
                        " 9 | 9 | 9 |");
    }

    @Test
    public void testANYSubqueryWithMutipleTablesNegativeTest() throws Exception {
        String sql = "select * from t1 where a1 in (select a2 from t2, t3 where b2=1 and c2=c3)";
        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 | 1 | 1 |");
    }

    @Test
    public void testExplainListingSubqueryOnce() throws Exception {
        String sql = "select * from t1 --splice-properties index=idx_t1\n" +
                "where c1 = (select a3 from t3 where a3=3)";
        assertUnorderedResult(sql, ONE_SUBQUERY_NODE,
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 3 | 3 | 3 |");
    }
    /**************************************************************************************/

    private void assertUnorderedResult(String sql, int expectedSubqueryCountInPlan, String expectedResult) throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                sql, expectedSubqueryCountInPlan, expectedResult
        );
    }
}
