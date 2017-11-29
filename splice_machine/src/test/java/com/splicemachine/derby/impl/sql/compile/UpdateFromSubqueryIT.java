/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 11/27/17.
 */
@RunWith(Parameterized.class)
public class UpdateFromSubqueryIT extends SpliceUnitTest {
    private static final String SCHEMA = UpdateFromSubqueryIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(8);
        params.add(new Object[]{"NESTEDLOOP","true"});
        params.add(new Object[]{"SORTMERGE","true"});
        params.add(new Object[]{"BROADCAST","true"});
    //  params.add(new Object[]{"MERGE","true"});    //merge join path is disabled due to wrong result tracked in DB-6320
        params.add(new Object[]{"NESTEDLOOP","false"});
        params.add(new Object[]{"SORTMERGE","false"});
        params.add(new Object[]{"BROADCAST","false"});
    //  params.add(new Object[]{"MERGE","false"});   //merge join path is disabled due to wrong result tracked in DB-6320
        return params;
    }

    private String joinStrategy;
    private String useSparkString;

    public UpdateFromSubqueryIT(String joinStrategy, String useSparkString) {
        this.joinStrategy = joinStrategy;
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, primary key(a1,d1))")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                        row(2, 2, 2, 2),
                        row(3, 3, 3, 3),
                        row(1, 1, 1, 11),
                        row(2, 2, 2, 21))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, primary key (a2,d2))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(row(1, 10, 10, 10),
                        row(3, 30, 30, 30),
                        row(4, 40, 40, 40))).create();

    }

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=spliceClassWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        try {
            conn.rollback();
        } catch (Exception e) {} // Swallow for HFile Bit Running in Control
    }

    @Test
    public void testUpdateFromSpliceTable() throws Exception {
        spliceClassWatcher.executeUpdate(format("update t1 set (b1) = (select b2 from t2 --splice-properties joinStrategy=%s,useSpark=%s\n" +
                "where a1=a2)", this.joinStrategy, this.useSparkString));

        String sql = "select * from t1";

        String expected = "A1 |B1 |C1 |D1 |\n" +
                "----------------\n" +
                " 1 |10 | 1 | 1 |\n" +
                " 1 |10 | 1 |11 |\n" +
                " 2 | 2 | 2 | 2 |\n" +
                " 2 | 2 | 2 |21 |\n" +
                " 3 |30 | 3 | 3 |";
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testUpdateFromVTITable() throws Exception {
        try {
            spliceClassWatcher.executeUpdate(format("update t1 set (b1) = (select b2 from " +
                            "new com.splicemachine.derby.vti.SpliceFileVTI('%s',NULL,',',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-ddHH:mm:ss.SSZ','true','UTF-8') AS importVTI (a2 INTEGER, b2 INTEGER, C2 CHAR(24)) --splice-properties joinStrategy=%s, useSpark=%s\n" +
                            "WHERE a1 = importVTI.a2 - 30)",
                    SpliceUnitTest.getResourceDirectory() + "t1_part2.csv",
                    this.joinStrategy, this.useSparkString));

            String sql = "select * from t1";

            String expected = "A1 |B1 |C1 |D1 |\n" +
                    "----------------\n" +
                    " 1 |31 | 1 | 1 |\n" +
                    " 1 |31 | 1 |11 |\n" +
                    " 2 |32 | 2 | 2 |\n" +
                    " 2 |32 | 2 |21 |\n" +
                    " 3 |33 | 3 | 3 |";
            ResultSet rs = spliceClassWatcher.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            if (this.joinStrategy == "MERGE")
                Assert.assertEquals("Upexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
            else
                Assert.fail("Unexpected failure for join strategy: " + this.joinStrategy);
        }
    }
}
