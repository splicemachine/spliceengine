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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/7/14
 * Time: 1:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class LastIndexKeyOperationIT extends SpliceUnitTest {

    public static final String CLASS_NAME = LastIndexKeyOperationIT.class.getSimpleName().toUpperCase();

    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final String TABLE_NAME = "TAB";
    protected static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String tableDef = "(I INT, D DOUBLE, primary key (i))";
    protected static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);
    static final int MAX=10;
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher));
                        for(int i=1;i<MAX+1;i++){
                            ps.setInt(1,i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, primary key (c1))")
                .withIndex("create index idx_t1 on t1(a1)")
                .withIndex("create index idx2_t1 on t1(b1 desc, a1)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(row(1, 10, 100), row(null, null, -1), row(2, 20, 200), row(3, 30, 300))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, primary key (c2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(row(1, 10, 100), row(null, null, -1), row(2, 20, 200), row(3, 30, 300))).create();

        try (Statement st = connection.createStatement()) {
            st.execute(String.format("CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'T1', 100000, 100, 20)", CLASS_NAME));
            st.execute(String.format("CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'T2', 100000, 100, 20)", CLASS_NAME));
        }
        connection.commit();
    }
    /**
     * This '@Before' method is ran before every '@Test' method
     */
    @Before
    public void setUp() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(
                String.format("select * from %s", this.getTableReference(TABLE_NAME)));
        Assert.assertEquals(MAX, resultSetSize(resultSet));
        resultSet.close();
    }

    @Test
    public void testLastIndexKey() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("select max(i) from %s", this.getTableReference(TABLE_NAME)));

        while(rs.next()){
            Assert.assertEquals(MAX,rs.getInt(1));
        }
        rs.close();

    }

    @Test
    public void testMaxOnPK() throws Exception {
        String sqlText = "select max(c1) from t1";
        String expected = "1  |\n" +
                "-----\n" +
                "300 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLTP (default)"},
                new String[]{"LastKeyTableScan", "scannedRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMaxOnPKUsingSpark() throws Exception {
        String sqlText = "select max(c1) from t1 --splice-properties useSpark=true";
        String expected = "1  |\n" +
                "-----\n" +
                "300 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (query hint)"},
                new String[]{"TableScan", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMinOnPK() throws Exception {
        String sqlText = "select min(c1) from t1";
        String expected = "1 |\n" +
                "----\n" +
                "-1 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (cost)"},
                new String[]{"TableScan", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMaxOnIndex() throws Exception {
        String sqlText = "select max(a1) from t1";
        String expected = "1 |\n" +
                "----\n" +
                " 3 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLTP (default)"},
                new String[]{"LastKeyIndexScan", "scannedRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMaxOnIndexUsingSpark() throws Exception {
        String sqlText = "select max(a1) from t1 --splice-properties useSpark=true";
        String expected = "1 |\n" +
                "----\n" +
                " 3 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (query hint)"},
                new String[]{"IndexScan[IDX_T1", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMinOnIndex() throws Exception {
        String sqlText = "select min(a1) from t1";
        String expected = "1 |\n" +
                "----\n" +
                " 1 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (cost)"},
                new String[]{"IndexScan[IDX_T1", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMinOnIndexWithDescOrder() throws Exception {
        String sqlText = "select min(b1) from t1";
        String expected = "1 |\n" +
                "----\n" +
                "10 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLTP (default)"},
                new String[]{"LastKeyIndexScan[IDX2_T1", "scannedRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMaxOnIndexWithDescOrder() throws Exception {
        String sqlText = "select max(b1) from t1";
        String expected = "1 |\n" +
                "----\n" +
                "30 |";

        rowContainsQuery(new int[]{1,6}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (cost)"},
                new String[]{"IndexScan[IDX2_T1", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMaxInDerivedTable() throws Exception {
        String sqlText = "select * from t2, (select max(c1) as M from t1) dt where dt.M=c2";
        String expected = "A2 |B2 |C2  | M  |\n" +
                "------------------\n" +
                " 3 |30 |300 |300 |";

        rowContainsQuery(new int[]{1,4, 5, 9}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLTP (default)"},
                new String[] {"NestedLoopJoin"},
                new String[] {"TableScan[T2"},
                new String[]{"LastKeyTableScan[T1", "scannedRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMinInSubquery() throws Exception {
        // we need to scan t2 with 100k rows, so spark path is control and SpecialMaxScan
        // cannot be used
        String sqlText = "select * from t2 where b2 in (select min(b1) as M from t1)";
        String expected = "A2 |B2 |C2  |\n" +
                "-------------\n" +
                " 1 |10 |100 |";

        rowContainsQuery(new int[]{1, 4, 5, 9}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLAP (cost)"},
                new String[] {"TableScan[T2", "scannedRows=100000,outputRows=1"},
                new String[] {"Subquery"},
                new String[]{"IndexScan[IDX2_T1", "scannedRows=100000"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMinInSubquery2() throws Exception {
        // without using stats on t2, we only scan 20 t2 rows, so control path can be taken, so is SpecialMaxScan too
        String sqlText = "select * from t2 --splice-properties skipStats=true\n " +
                "where b2 in (select min(b1) as M from t1)";
        String expected = "A2 |B2 |C2  |\n" +
                "-------------\n" +
                " 1 |10 |100 |";

        rowContainsQuery(new int[]{1, 4, 5, 9}, "explain " + sqlText, methodWatcher,
                new String[] {"engine=OLTP (default)"},
                new String[] {"TableScan[T2", "scannedRows=20,outputRows=2"},
                new String[] {"Subquery"},
                new String[]{"LastKeyIndexScan[IDX2_T1", "scannedRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
