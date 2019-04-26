/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 4/27/17.
 */
public class UnsatTreePruningIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(UnsatTreePruningIT.class);
    public static final String CLASS_NAME = UnsatTreePruningIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 varchar(10), c1 float)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();


        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 varchar(10), c2 float)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 varchar(10), c3 float)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 varchar(10), c4 float)")
                .withInsert("insert into t4 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    public void assertPruneResult(String query,
                                  String expected,
                                  boolean shouldRewrite) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        assertEquals("\n" + query + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        ResultSet rs2 = methodWatcher.executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs2);
        if (shouldRewrite)
            Assert.assertTrue("Query is expected to be pruned but not", explainPlanText.contains("Values"));
        else
            Assert.assertTrue("Query is not expected to be pruned but is", !explainPlanText.contains("Values"));

        rs2.close();
    }

    public void assertPruneResultForDML(String query,
                                        int rowAffected,
                                        String verificationQuery,
                                        String expected,
                                        boolean shouldRewrite) throws Exception {
        int num = methodWatcher.executeUpdate(query);
        assertEquals("incorrect number of records updated!", rowAffected, num);

        ResultSet rs = methodWatcher.executeQuery(verificationQuery);
        assertEquals("\n" + query + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs2);
        if (shouldRewrite)
            Assert.assertTrue("Query is expected to be pruned but not", explainPlanText.contains("Values"));
        else
            Assert.assertTrue("Query is not expected to be pruned but is", !explainPlanText.contains("Values"));

        rs2.close();
    }

    @Test
    public void testMainQueryWithUnsat() throws Exception {
        /* case1 */
        /* base case */
        String sqlText = "select a1,a2,b1,b2,c1,c2 from t1, t2 where a1=a2 and 1=0 and b1='aaa'";

        String expected = "";


        assertPruneResult(sqlText, expected, true);

        /* case 2 */
        /* base case with outer join */
        sqlText = "select a1,a2,b1,b2,c1,c2 from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa'";

        expected = "";


        assertPruneResult(sqlText, expected, true);

        /* case3 */
        /* base case with multiple outer joins */
        sqlText = "select a1, a2, a3, a4 from t1 left join t2 on a1=a2 right join t3 on a1=a3 right join t4 on a3=a4 where 1=0";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 4 */
        /* query with aggregation but no group by */
        sqlText = "select count(*) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa'";

        expected =
                "1 |\n" +
                        "----\n" +
                        " 0 |";


        assertPruneResult(sqlText, expected, true);

        /* case 5 */
        /* query with aggregation and no group by but with having */
        sqlText = "select count(*) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa' having count(c2)=0";

        expected =
                "1 |\n" +
                        "----\n" +
                        " 0 |";


        assertPruneResult(sqlText, expected, true);

        /* case 6 */
        /* query with group by */
        sqlText = "select sum(c2) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa' group by a1";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 7 */
        /* query with group by and having */
        sqlText = "select sum(c2) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa' group by a1 having a1=1 and count(*)=1";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 8 */
        /* query with group by and order by */
        sqlText = "select a1, sum(a2) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1=b2 group by a1 order by 1";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 9 */
        /* query with group by + order by + limit */
        sqlText = "select a1, sum(a2) from t1 left join t2 on a1=a2 and a1=1 where 1=0 and b1='aaa' group by a1 order by 1 {limit 1}";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 10 */
        /* query with flattened where subqueries */
        sqlText = "select a1 from t1 where a1 in (select a2 from t2 where b1=b2) and 1=0";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 11 */
        /* query with non-flattened where subqueries */
        sqlText = "select a1 from t1 where a1 in (select sum(a2) from t2 group by b2) and 1=0";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 12 */
        /* query with having subqueries */
        sqlText = "select a1,sum(c2) from t1, t2 where a1=a2 and 1=0 group by a1 having a1 in (select a3 from t3, t4 where a3=a4)";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 13 */
        /* query with having subqueries 2 */
        sqlText = "select count(*) from t1, t2 where a1=a2 and 1=0 having count(a1) < (select min(a3) from t3, t4 where a3=a4)";
        expected = "1 |\n" +
                "----\n" +
                " 0 |";

        assertPruneResult(sqlText, expected, true);

        /* case 14 */
        /* query with select subqueries */
        sqlText = "select a1, (select a2 from t2 where a1=a2) from t1 where 1=0";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        return;
    }

    @Test
    public void testDerivedTableWithUnsat() throws Exception {
        /* case 1 */
        /* base DT in inner join */
        String sqlText = "select a1 from t1, (select b2 from t2 where 1=0)dt(b2) where b1=dt.b2";

        String expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 2 */
        /* DT in outer join */
        sqlText = "select a1,a2,b1,b2,c1,c2 from (select * from t1 where 1=0)dt left join t2 on a1=a2 and a1=1 where b1='aaa'";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 3 */
        /* DT in outer join 2 */
        sqlText = "select a1,c2 from t1 left join (select b2, c2 from t2,t3 where b2=b3 and 1=0)dt on b1=b2 order by 1";

        expected =
                "A1  | C2  |\n" +
                        "------------\n" +
                        "  1  |NULL |\n" +
                        "  2  |NULL |\n" +
                        "NULL |NULL |";

        assertPruneResult(sqlText, expected, true);

        /* case 4 */
        /* DT with aggregation */
        sqlText = "select a1,b1, dt.cc from t1, (select count(*) from t2 where 1=0)dt(cc) where a1>dt.cc order by 1";

        expected =
                "A1 |B1  |CC |\n" +
                        "-------------\n" +
                        " 1 |aaa | 0 |\n" +
                        " 2 |bbb | 0 |";

        assertPruneResult(sqlText, expected, true);

        return;
    }

    @Test
    public void testSubqueryWithUnsat() throws Exception {
        /* case 1 */
        /* where subquery, non-correlated */
        String sqlText = "select a1 from t1 where a1 in (select a2 from t2 where 1=0)";

        String expected = "";


        assertPruneResult(sqlText, expected, true);

        /* case 2 */
        /* where subquery, non-correlated, with negation (NOT IN) */
        sqlText = "select a1 from t1 where a1 not in (select a2 from t2 where 1=0) order by 1";

        expected =
                "A1  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";


        assertPruneResult(sqlText, expected, true);

        /* case 3 */
        /* where subquery, non-correlated, with negation (NOT EXISTS)  */
        sqlText = "select a1 from t1 where not exists (select a2 from t2 where 1=0) order by 1";

        expected =
                "A1  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";


        assertPruneResult(sqlText, expected, true);

        /* case 4 */
        /* where subquery, non-correlated, with negation (NOT EXISTS) 2 */
        /* for this case, pruning should not be triggered as not exists is not flattened, and anti-join is used */
        sqlText = "select a1 from t1,t3 where a1=a3 and not exists (select a2 from t2 where 1=0) order by 1";

        expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";


        assertPruneResult(sqlText, expected, false);

        /* case 5 */
        /* where subquery, correlated, in subquery without aggregate */
        sqlText = "select a1 from t1 where a1 in (select a2 from t2 where b1=b2 and 1=0)";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 6 */
        /* in subquery with aggregate */
        sqlText = "select a1 from t1 where a1 in (select count(a2)+1 from t2 where b1=b2 and 1=0)";

        expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |";


        assertPruneResult(sqlText, expected, true);

        /* case 7 */
        /* where subquery, correlated, with negation */
        sqlText = "select a1 from t1 where a1 not in (select a2 from t2 where b1=b2 and 1=0) order by 1";

        expected =
                "A1  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";


        assertPruneResult(sqlText, expected, true);

        /* case 8 */
        /* where subquery, non-correlated, with negation and aggregation  */
        sqlText = "select a1 from t1 where a1 not in (select count(a2)+1 from t2 where b1=b2 and 1=0)";

        expected =
                "A1 |\n" +
                        "----\n" +
                        " 2 |";


        assertPruneResult(sqlText, expected, true);

        /* case 9 */
        /* where subquery, not exists with correlation */
        /* correlated exists with a single table is no longer rewritten to a DT, instead it will be flattened as a FromBaseTable node,
           as a result, Unsat tree pruning does not kick in
         */
        sqlText = "select a1 from t1 where not exists (select a2 from t2 where a1=a2 and 1=0) order by 1";

        expected =
                "A1  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";

        assertPruneResult(sqlText, expected, true);

        /* case 9-1 */
        sqlText = "select a1 from t1 where not exists (select X.a2 from t2 as X, t2 as Y where a1=X.a2 and X.a2=Y.a2 and 1=0) order by 1";

        expected =
                "A1  |\n" +
                        "------\n" +
                        "  1  |\n" +
                        "  2  |\n" +
                        "NULL |";

        assertPruneResult(sqlText, expected, true);

        /* case 10 */
        /* where subquery, not exists with correlation */
        /* pruning should not be triggered as this is not exists subquery without flattening */
        sqlText = "select a1 from t1,t3 where a1=a3 and not exists (select a2 from t2 where a1=a2 and 1=0) order by 1";

        expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";


        assertPruneResult(sqlText, expected, false);

        /* case 11 */
        /* unsat subquery in Select clause */
        sqlText = "select a1, (select a1 from t2 where a1=a2 and 1=0) from t1 order by 1";

        expected =
                "A1  |  2  |\n" +
                        "------------\n" +
                        "  1  |NULL |\n" +
                        "  2  |NULL |\n" +
                        "NULL |NULL |";


        assertPruneResult(sqlText, expected, true);

        /* case 12 */
        /* unsat subquery in Select clause with aggregation */
        sqlText = "select a1, (select count(a2) from t2 where a1=a2 and 1=0) from t1 order by 1";

        expected =
                "A1  | 2 |\n" +
                        "----------\n" +
                        "  1  | 0 |\n" +
                        "  2  | 0 |\n" +
                        "NULL | 0 |";


        assertPruneResult(sqlText, expected, true);

        /* case 13 */
        /* unsat subquery in Having clause without aggregation */
        sqlText = "select a1,sum(c2) from t1, t2 where a1=a2 group by a1 having a1 in (select a3 from t3, t4 where a3=a4 and 1=0)";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 14 */
        /* unsat subquery in Having clause with aggregation */
        sqlText = "select count(*) from t1, t2 where a1=a2 having count(a1) = (select count(a3)+2 from t3, t4 where a3=a4 and 1=0)";

        expected =
                "1 |\n" +
                        "----\n" +
                        " 2 |";

        assertPruneResult(sqlText, expected, true);

        return;
    }

    @Test
    public void testSetOperationWithUnsat() throws Exception {
        /* case 1 */
        /* union all */
        String sqlText = " select * from t1, t2 where a1=a2 and 1=0 union all select * from t3,t4 where a3=a4 order by 1";

        String expected =
                "1 | 2  | 3  | 4 | 5  | 6  |\n" +
                        "----------------------------\n" +
                        " 1 |aaa |1.0 | 1 |aaa |1.0 |\n" +
                        " 2 |bbb |2.0 | 2 |bbb |2.0 |";


        assertPruneResult(sqlText, expected, true);

        /* case 2 */
        /* union all 2*/
        sqlText = "select * from t1, t2 where a1=a2 union all select * from t3,t4 where a3=a4 and 1=0 order by 1";

        expected =
                "1 | 2  | 3  | 4 | 5  | 6  |\n" +
                        "----------------------------\n" +
                        " 1 |aaa |1.0 | 1 |aaa |1.0 |\n" +
                        " 2 |bbb |2.0 | 2 |bbb |2.0 |";

        assertPruneResult(sqlText, expected, true);

        /* case 3 */
        /* union */
        sqlText = "select * from t1, t2 where a1=a2 and 1=0 union select * from t3,t4 where a3>a4";

        expected =
                "1 | 2  | 3  | 4 | 5  | 6  |\n" +
                        "----------------------------\n" +
                        " 2 |bbb |2.0 | 1 |aaa |1.0 |";


        assertPruneResult(sqlText, expected, true);

        /* case 4 */
        /* except */
        sqlText = "select * from t1, t2 where a1=a2 and 1=0 except select * from t3,t4 where a3=a4";

        expected = "";


        assertPruneResult(sqlText, expected, true);

        /* case 5 */
        /* except 2 */
        sqlText = "select a1,b1 from t1, t2 except select a3,b3 from t3,t4 where a3=a4 and 1=0 order by 1";

        expected =
                "1  |  2  |\n" +
                        "------------\n" +
                        "  1  | aaa |\n" +
                        "  2  | bbb |\n" +
                        "NULL |NULL |";

        assertPruneResult(sqlText, expected, true);

        /* case 6 */
        /* intersect */
        sqlText = "select * from t1, t2 where a1=a2 and 1=0 intersect select * from t3,t4 where a3=a4";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        return;
    }

    @Test
    public void testUpdateOperationWithUnsat() throws Exception {
        /* case 1 */
        /* no row update */
        String sqlText = "update t1 set a1=1 where 'xxxx'='y' and b1 in (select b2 from t2)";
        String verificationQuery = "select * from t1 order by 1,2,3";

        String expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        /* case 2 */
        /* no row update */
        sqlText = "update t1 set a1=1 where b1 in (select b2 from t2 where 1=0)";

        expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        return;
    }

    @Test
    public void testInsertSelectOperationWithUnsat() throws Exception {
        /* case 1 */
        /* no row inserted */
        String sqlText = "insert into t1 select * from t2 where 1=0";
        String verificationQuery = "select * from t1 order by 1,2,3";

        String expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        /* case 2 */
        /* no row inserted */
        sqlText = "insert into t1 select a2, b2, c2 from t2, t1 where 1=0";

        expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        /* case 3 */
        /* no row inserted */
        sqlText = "insert into t1 select a2, b2, c2 from t2 left join t1 on a2=a1 where 1=0";

        expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        /* case 4 */
        /* no row inserted */
        sqlText = "insert into t1 select a2, b2, c2 from t2 where a2 in (select a3 from t3 where 1=0)";

        expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);


        return;
    }

    @Test
    public void testDeleteOperationWithUnsat() throws Exception {
        /* case 1 */
        /* no row deleted */
        String sqlText = "delete from t1 where 1=0 and b1 in (select b2 from t2)";
        String verificationQuery = "select * from t1 order by 1,2,3";

        String expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        /* case 2 */
        /* no row deleted */
        sqlText = "delete from t1 where b1 in (select b2 from t2 where 1=0)";

        expected =
                "A1  | B1  | C1  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "NULL |NULL |NULL |";


        assertPruneResultForDML(sqlText, 0, verificationQuery, expected, true);

        return;
    }
}
