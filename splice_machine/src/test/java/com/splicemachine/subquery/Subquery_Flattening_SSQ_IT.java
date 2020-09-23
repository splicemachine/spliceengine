/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by yxia on 10/16/17.
 */
@RunWith(Parameterized.class)
public class Subquery_Flattening_SSQ_IT extends SpliceUnitTest {
    private static final String SCHEMA = Subquery_Flattening_SSQ_IT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(8);
        params.add(new Object[]{"NESTEDLOOP","true"});
        params.add(new Object[]{"SORTMERGE","true"});
        params.add(new Object[]{"BROADCAST","true"});
        params.add(new Object[]{"MERGE","true"});
        params.add(new Object[]{"NESTEDLOOP","false"});
        params.add(new Object[]{"SORTMERGE","false"});
        params.add(new Object[]{"BROADCAST","false"});
        params.add(new Object[]{"MERGE","false"});
        return params;
    }

    private String joinStrategy;
    private String useSparkString;

    public Subquery_Flattening_SSQ_IT(String joinStrategy, String useSparkString) {
        this.joinStrategy = joinStrategy;
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, primary key(d1))")
                .withIndex("create index idx_t1 on t1 (a1,b1,d1)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                               row(1, 1, 1, 11),
                               row(2, 2, 2, 2),
                               row(3, 3, 3, 3),
                               row(4, 4, 4, 4))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, primary key(d2))")
                .withIndex("create index idx_t2 on t2 (a2,b2,d2)")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                        row(2, 2, 2, 22),
                        row(2, 2, 2, 2),
                        row(4, 4, 4, 4))).create();

    }

    @Test
    public void testSSQflattenedToBaseTable() throws Exception {
        /* Q1 regular test case, query runs*/
        String sql = String.format(
                "select d1, (select d2 from  t2 --splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where a1 = a2 and a2<>2) as D from t1", this.joinStrategy,this.useSparkString);

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2 regular test case, query runs, top 1 without order by*/
        sql = String.format(
                "select d1, (select top 1 b2 from  t2 --splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where a1 = a2) as B from t1", this.joinStrategy,this.useSparkString);

        expected = "D1 |  B  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |  2  |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q3 negative test case, error is reported */
        try {
            methodWatcher.executeQuery(String.format(
                    "select d1, (select d2 from  t2 --splice-properties joinStrategy=%s, useSpark=%s\n" +
                            "where a1 = a2 and a2=2) as D from t1", this.joinStrategy, this.useSparkString
            ));
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }
    }

    @Test
    public void testSSQflattenedToBaseTableWithCorrelationInSingleTableCondition() throws Exception {
        /* test correlation in "a1<>2" where a1 is from the outer table */
        String sql = String.format(
                "select d1, (select d2 from  t2 --splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where a1 = a2 and a1<>2 and b2<2) as D from t1", this.joinStrategy,this.useSparkString);

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |NULL |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testSSQflattenedToBaseTableWithCorrelationInInListCondition() throws Exception {
        String sql = String.format(
                "select d1, (select d2 from  t2 --splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where a1 = a2 and a1 in (1,3,4) and b2>=3) as D from t1", this.joinStrategy,this.useSparkString);

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testSSQWithCorrelationInORConditionNotFlattened() throws Exception {
        String sql = String.format(
                "select d1, (select d2 from  t2 --splice-properties useSpark=%s\n" +
                        "where a1 = a2 and (a1=1 or a1=3 or a1=4) and b2>=3) as D from t1", this.joinStrategy,this.useSparkString);

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

}
