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
package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 2/1/18.
 */
@RunWith(Parameterized.class)
public class NonCoveringIndexIT extends SpliceUnitTest {
    private static final String SCHEMA = NonCoveringIndexIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(8);
        params.add(new Object[]{"NESTEDLOOP"});
        params.add(new Object[]{"SORTMERGE"});
        params.add(new Object[]{"BROADCAST"});
        return params;
    }

    private String joinStrategy;

    public NonCoveringIndexIT(String joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int)")
                .withIndex("create index ix_t2 on t2 (b2,c2)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(row(3, 3, 3),
                        row(4, 4, 4),
                        row(4, 5, 5))).create();
    }

    @Test
    public void testJoinWithNonCoveringIndexInDTAndInnerJoin() throws Exception {
        String sql = format("select * from --splice-properties joinOrder=fixed\n" +
                "t1,\n" +
                "(select * from\n" +
                "  t2  --splice-properties index=ix_t2\n" +
                " where b2=4) dt --splice-properties joinStrategy=%s\n" +
                "where a1=a2", this.joinStrategy);

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |";
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinWithNonCoveringIndexInDTAndNonIndexPredicate() throws Exception {
        String sql = format("select * from --splice-properties joinOrder=fixed\n" +
                "t1,\n" +
                "(select * from\n" +
                "  t2  --splice-properties index=ix_t2\n" +
                " where b2>1 and a2>3) dt --splice-properties joinStrategy=%s\n" +
                "where a1=a2", this.joinStrategy);

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 4 | 4 | 4 | 4 | 4 | 4 |\n" +
                " 4 | 4 | 4 | 4 | 5 | 5 |";
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinWithNonCoveringIndexInDTAndInListPredicate() throws Exception {
        String sql = format("select * from --splice-properties joinOrder=fixed\n" +
                "t1,\n" +
                "(select * from\n" +
                "  t2  --splice-properties index=ix_t2\n" +
                " where b2 in (1,2,3) and c2 in (2,3,4)) dt --splice-properties joinStrategy=%s\n" +
                "where a1=a2", this.joinStrategy);

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 3 | 3 | 3 | 3 | 3 | 3 |";
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }
    @Test
    public void testJoinWithNonCoveringIndexInDTAndOuterJoin() throws Exception {
        String sql = format("select * from --splice-properties joinOrder=fixed\n" +
                "t1 left join \n" +
                "(select * from\n" +
                "  t2  --splice-properties index=ix_t2\n" +
                " where b2=4) dt --splice-properties joinStrategy=%s\n" +
                "on a1=a2", this.joinStrategy);

        String expected = "A1 |B1 |C1 | A2  | B2  | C2  |\n" +
                "------------------------------\n" +
                " 1 | 1 | 1 |NULL |NULL |NULL |\n" +
                " 2 | 2 | 2 |NULL |NULL |NULL |\n" +
                " 3 | 3 | 3 |NULL |NULL |NULL |\n" +
                " 4 | 4 | 4 |  4  |  4  |  4  |";
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testJoinWithNonCoveringIndex() throws Exception {
        try {
            String sql = format("select * from --splice-properties joinOrder=fixed\n" +
                    "t1,\n" +
                    "t2  --splice-properties index=ix_t2 ,joinStrategy=%s\n" +
                    "where c1=c2 and b2 = 3", this.joinStrategy);

            String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                    "------------------------\n" +
                    " 3 | 3 | 3 | 3 | 3 | 3 |";
            ResultSet rs = spliceClassWatcher.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            if (this.joinStrategy == "BROADCAST")
                // broadcast join does not support non-covering index as right table
                Assert.assertEquals("Upexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
            else
                Assert.fail("Unexpected failure for join strategy: " + this.joinStrategy);
        }
    }
}
