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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 4/14/20.
 */
@RunWith(Parameterized.class)
@Category({HBaseTest.class})
public class JoinDeleteOperationIT extends SpliceUnitTest {

    private static final String SCHEMA = JoinDeleteOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static String BULKLOADDIR;

    private static String[] JOINS = {"NESTEDLOOP", "SORTMERGE", "BROADCAST", "MERGE"};
    private static int[] DELETE_PATH = {1, 2, 3}; /* 1: control path; 2: spark path; 3: bulk delete */
    private static int[] INDEX = {1,2,3}; /* 1: no index, 2: use as a covering index; 3: use as a non-covering index */

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(192);
        for (int deletePath: DELETE_PATH) {
            for (int useIndex: INDEX) {
                for (String joinStrategy: JOINS) {
                     params.add(new Object[] {deletePath,
                                              useIndex,
                                              joinStrategy});
                }
            }
        }
        return params;
    }


    private String joinStrategy;
    private int deletePath;
    private int useIndex;

    public JoinDeleteOperationIT(int deletePath, int useIndex, String joinStrategy) {
        this.joinStrategy = joinStrategy;
        this.deletePath = deletePath;
        this.useIndex = useIndex;
    }


    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        BULKLOADDIR = SpliceUnitTest.createBulkLoadDirectory(SCHEMA).getCanonicalPath();

        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1_nopk (a1 int, b1 varchar(10), c1 int)")
                .withIndex("create index idx_t1_nopk on t1_nopk(c1, b1)")
                .withIndex("create index idx_t1_nopk2 on t1_nopk(b1)")
                .withInsert("insert into t1_nopk values(?,?,?)")
                .withRows(rows(row(1, "a", 1), row(1, "aa", 11), row(2, "b", 2), row(3, "c", 3))).create();

        new TableCreator(connection)
                .withCreate("create table t1_pk (a1 int, b1 varchar(10), c1 int, primary key(c1))")
                .withIndex("create index idx_t1_pk on t1_pk(c1, b1)")
                .withIndex("create index idx_t1_pk2 on t1_pk(b1)")
                .withInsert("insert into t1_pk values(?,?,?)")
                .withRows(rows(row(1, "a", 1), row(1, "aa", 11), row(2, "b", 2), row(3, "c", 3))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 varchar(10), c2 int)")
                .withIndex("create index idx_t2 on t2(c2, b2)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(row(1, "aA", 1), row(2, "bB", 2))).create();
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testDeleteFromNoPKTable() throws Exception{
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA); //just in case the schema wasn't set already
        try(Statement s = conn.createStatement()){
            String deletePathString = "useSpark=false";
            if (deletePath == 2) {
                deletePathString = "useSpark=true";
            } else if (deletePath == 3) {
                deletePathString = "bulkDeleteDirectory='" + BULKLOADDIR + "'";
            }

            String indexString = "null";
            if  (useIndex == 2) {
                indexString = "idx_t1_nopk";
            } else if (useIndex == 3) {
                indexString = "idx_t1_nopk2";
            }
            try {
                String sql = format("delete from t1_nopk --splice-properties index=%s, %s\n" +
                                "where exists (select 1 from t2 --splice-properties joinStrategy=%s\n" +
                                "where c1=c2)",
                        indexString, deletePathString, joinStrategy);

                int rowsDeleted = s.executeUpdate(sql);
                Assert.assertEquals(format("The combination (%s, index=%s, %s):", deletePathString, indexString, joinStrategy) + "Row deleted counts does not match, expected 2, actual is " + rowsDeleted, 2, rowsDeleted);

                ResultSet rs = s.executeQuery("select * from t1_nopk --splice-properties index=null");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy), "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 |aa |11 |\n" +
                        " 3 | c | 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();

                rs = s.executeQuery("select b1,c1 from t1_nopk --splice-properties index=idx_t1_nopk");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy),"B1 |C1 |\n" +
                        "--------\n" +
                        "aa |11 |\n" +
                        " c | 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();

                rs = s.executeQuery("select b1 from t1_nopk --splice-properties index=idx_t1_nopk2");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy),"B1 |\n" +
                        "----\n" +
                        "aa |\n" +
                        " c |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            } catch (SQLException e) {
                // only merge join may hit no plan error
                if (joinStrategy.equals("MERGE") && useIndex!=2) {
                    Assert.assertTrue(format("The combination (%s, index=%s, %s) should not fail with the error :", deletePathString, indexString, joinStrategy) + e.getMessage(), e.getSQLState().equals(SQLState.LANG_NO_BEST_PLAN_FOUND));
                } else {
                    Assert.fail(format("The combination (%s, index=%s, %s) should not fail with the error :", deletePathString, indexString, joinStrategy) + e.getMessage());
                }
            }
        }finally{
            conn.rollback();
            conn.reset();
        }
    }

    @Test
    public void testDeleteFromPKTable() throws Exception{
        // ignore the combination bulk delete + index lookup scenarios (DB-9402)
        if (deletePath == 3 && useIndex==3)
            return;

        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA); //just in case the schema wasn't set already
        try(Statement s = conn.createStatement()){
            String deletePathString = "useSpark=false";
            if (deletePath == 2) {
                deletePathString = "useSpark=true";
            } else if (deletePath == 3) {
                deletePathString = "bulkDeleteDirectory='" + BULKLOADDIR + "'";
            }

            String indexString = "null";
            if  (useIndex == 2) {
                indexString = "idx_t1_pk";
            } else if (useIndex == 3) {
                indexString = "idx_t1_pk2";
            }
            try {
                String sql = format("delete from t1_pk --splice-properties index=%s, %s\n" +
                                "where exists (select 1 from t2 --splice-properties joinStrategy=%s\n" +
                                "where c1=c2)",
                        indexString, deletePathString, joinStrategy);

                int rowsDeleted = s.executeUpdate(sql);
                Assert.assertEquals(format("The combination (%s, index=%s, %s):", deletePathString, indexString, joinStrategy) + "Row deleted counts does not match, expected 2, actual is " + rowsDeleted, 2, rowsDeleted);

                ResultSet rs = s.executeQuery("select * from t1_pk --splice-properties index=null");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy),"A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " 1 |aa |11 |\n" +
                        " 3 | c | 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();

                rs = s.executeQuery("select b1,c1 from t1_pk --splice-properties index=idx_t1_pk");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy),"B1 |C1 |\n" +
                        "--------\n" +
                        "aa |11 |\n" +
                        " c | 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();

                rs = s.executeQuery("select b1 from t1_pk --splice-properties index=idx_t1_pk2");
                assertEquals(format("The combination (%s, index=%s, %s) comparison failure", deletePathString, indexString, joinStrategy),"B1 |\n" +
                        "----\n" +
                        "aa |\n" +
                        " c |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            } catch (SQLException e) {
                // only merge join may hit no plan error
                if (joinStrategy.equals("MERGE") && useIndex==3) {
                    Assert.assertTrue(format("The combination (%s, index=%s, %s) should not fail with the error :", deletePathString, indexString, joinStrategy) + e.getMessage(), e.getSQLState().equals(SQLState.LANG_NO_BEST_PLAN_FOUND));
                } else {
                    Assert.fail(format("The combination (%s, index=%s, %s) should not fail with the error :", deletePathString, indexString, joinStrategy) + e.getMessage());
                }
            }
        }finally{
            conn.rollback();
            conn.reset();
        }
    }

}
