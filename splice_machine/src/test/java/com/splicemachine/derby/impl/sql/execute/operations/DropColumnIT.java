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

import com.splicemachine.derby.impl.sql.actions.index.CustomerTable;
import com.splicemachine.derby.test.framework.*;

import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/17/14
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class DropColumnIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(DropColumnIT.class);

    private static final String SCHEMA_NAME = DropColumnIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static int nRows = 0;
    private static int nCols = 0;

    private static String tableName = "TEST_TABLE";
    private static String indexName = "TEST_IDEXN";

    private static String tableWithConstraints = "TABLE_WITH_CONSTRAINTS";
    private static String tableWithConstraints2 = "TABLE_WITH_CONSTRAINTS2";

    @BeforeClass
    public static void prepare() throws Exception {
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int, d int, e int, f int, g int, primary key(c))", tableName))
                .withRows(rows(row(1,2,3,4,5,6,7)))
                .withInsert(format("insert into %s values (?,?,?,?,?,?,?)", tableName))
                .withIndex(format("create index %s on %s(c,f)", indexName, tableName)).create();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int, d int, primary key(c), unique(b))", tableWithConstraints))
                .withRows(rows(row(1,2,3,4)))
                .withInsert(format("insert into %s values (?,?,?,?)", tableWithConstraints))
                .create();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int references %s(c), d int)", tableWithConstraints2, tableName))
                .withRows(rows(row(1,2,3,4)))
                .withInsert(format("insert into %s values (?,?,?,?)", tableWithConstraints2))
                .create();
    }

    protected static CustomerTable customerTableWatcher = new CustomerTable(CustomerTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/customer.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    public int rowCount(String schemaName, String tableName) {
        int nrows = 0;
        try {
            ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s.%s ",schemaName, tableName));
            rs.next();
            nrows = rs.getInt(1);
            rs.close();
        } catch (Exception e) {
            // ignore the error
        }
        return nrows;
    }

    public int columnCount(String schemaName, String tableName) {
        int ncols = 0;
        try {
            ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s ",schemaName, tableName));
            rs.next();
            ResultSetMetaData rsmd = rs.getMetaData();
            ncols = rsmd.getColumnCount();
            rs.close();
        } catch (Exception e) {
            // ignore the error
        }
        return ncols;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(customerTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Before
    public void setup () {
        nRows = rowCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertNotEquals(0, nRows);

        nCols = columnCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertNotEquals(0, nCols);
    }

    @Test
    public void testDropColumn(){
        try {
            methodWatcher.prepareStatement("alter table DropColumnIT.customer drop column c_data").execute();
            int n = rowCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
            Assert.assertEquals(n, nRows);

            nCols -= 1;
            n = columnCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
            Assert.assertEquals(n, nCols);
        } catch (Exception e1) {
            // ignore
        }
    }

    @Test
    public void testDropPKColumn() throws Exception{
        Connection connection = methodWatcher.getOrCreateConnection();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet rs = dmd.getPrimaryKeys(null, SCHEMA_NAME, CustomerTable.TABLE_NAME);
        int nIndexCols = resultSetSize(rs);
        rs.close();
        // Drop PK column
        methodWatcher.prepareStatement("alter table DropColumnIT.customer drop column c_id").execute();
        int n = rowCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertEquals(nRows,n);

        nCols -= 1;
        n = columnCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertEquals(n, nCols);
        connection = methodWatcher.createConnection();
        dmd = connection.getMetaData();
        rs = dmd.getPrimaryKeys(null, SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertEquals(0, resultSetSize(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropIndexColumn() throws Exception{
        // Create indexes on customer table
        SpliceIndexWatcher.createIndex(methodWatcher.createConnection(),
                                       SCHEMA_NAME,
                                       CustomerTable.TABLE_NAME,
                                       CustomerTable.INDEX_NAME,
                                       CustomerTable.INDEX_ORDER_DEF_ASC,
                                       false);
        Connection connection = methodWatcher.getOrCreateConnection();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet rs = dmd.getIndexInfo(null, SCHEMA_NAME, CustomerTable.TABLE_NAME, false, true);
        int nIndexCols = resultSetSize(rs);
        rs.close();
        // Drop index column
        methodWatcher.prepareStatement("alter table DropColumnIT.customer drop column c_first").execute();
        int n = rowCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertEquals(n, nRows);

        nCols -= 1;
        n = columnCount(SCHEMA_NAME, CustomerTable.TABLE_NAME);
        Assert.assertEquals(n, nCols);
        connection = methodWatcher.getOrCreateConnection();
        dmd = connection.getMetaData();
        rs = dmd.getIndexInfo(null, SCHEMA_NAME, CustomerTable.TABLE_NAME, false, true);
        Assert.assertEquals(nIndexCols-1, resultSetSize(rs));
        rs.close();
    }

    private static void dropColumn(Statement s, String column) throws SQLException {
        s.execute(format("alter table %s drop column %s", tableName, column));
    }

    private static void addColumn(Statement s, String column) throws SQLException {
        s.execute(format("alter table %s add column %s int", tableName, column));
    }

    private static void select(Statement s, String columns, int... expectedValues) throws SQLException {
        try(ResultSet rs = s.executeQuery(format("select %s from %s", columns, tableName))) {
            Assert.assertTrue(rs.next());
            for(int i = 0; i < expectedValues.length; ++i) {
                Assert.assertEquals(expectedValues[i], rs.getInt(i+1));
            }
            Assert.assertFalse(rs.next());
        }
    }
    private static void addRow(Statement s, int... values) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for(int value : values) {
            sb.append(value).append(",");
        }
        sb.deleteCharAt(sb.length()-1);

        s.execute(format("insert into %s values (%s)", tableName, sb.toString()));
    }

    private static void emptyTable(Statement s) throws SQLException {
        s.execute(format("drop table %s", tableName));
    }

    @Test
    public void dropColumnWithIndex() throws Exception {

        /**
         * Cols   a, b, c, d, e, f, g
         * Idx          .        .
         * Val    1, 2, 3, 4, 5, 6, 7
         */

        try (Statement s = spliceClassWatcher.getOrCreateConnection().createStatement()) {
            select(s, "c,f", 3, 6);
            dropColumn(s, "d");
            select(s, "c,f", 3, 6);
            dropColumn(s, "e");
            select(s, "c,f", 3, 6);
            dropColumn(s, "g");
            select(s, "c,f", 3, 6);
            dropColumn(s, "b");
            select(s, "c,f", 3, 6);
            dropColumn(s, "a");
            select(s, "c,f", 3, 6);
        }
    }

    @Test
    public void testDropColumnAfterInsertion() throws Exception {
        methodWatcher.execute("create table t(i int, j bigint, k varchar(10), l real)");
        methodWatcher.execute(("insert into t values (1,2,'3', 4.0)"));
        methodWatcher.execute("alter table t drop j");
        methodWatcher.execute("create index ti on t(k, i)");
        String expected = "I | K | L  |\n" +
                "-------------\n" +
                " 1 | 3 |4.0 |";
        try (ResultSet rs = methodWatcher.executeQuery("select * from t --splice-properties index=ti")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }
        try (ResultSet rs = methodWatcher.executeQuery("select * from t --splice-properties index=null")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }
        String sqlText = "explain select i, k from t";
        testQueryContains(sqlText, "IndexScan", methodWatcher, true);
    }

    @Test
    public void testInsertAfterDrop() throws Exception {
        methodWatcher.execute("create table t1(a int, b int, c int, d int, e int, f int, g int, h int, z int)");
        methodWatcher.execute("alter table t1 drop column b");
        methodWatcher.execute("alter table t1 drop column c");
        methodWatcher.execute("alter table t1 drop column d");
        methodWatcher.execute("create index it1 on t1(a, z)");
        methodWatcher.execute("insert into t1 values(1,5,6,7,8,9)");
        methodWatcher.execute("alter table t1 drop column f");
        methodWatcher.execute("alter table t1 drop column g");
        methodWatcher.execute("alter table t1 drop column h");
        methodWatcher.execute("insert into t1 values (11,55,99)");
        String expected =
                "A | E | Z |\n" +
                "------------\n" +
                " 1 | 5 | 9 |\n" +
                "11 |55 |99 |";
        try (ResultSet rs = methodWatcher.executeQuery("select * from t1 --splice-properties index=it1 \n" +
                "order by 1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }
        try (ResultSet rs = methodWatcher.executeQuery("select * from t1 --splice-properties index=null \n " +
                "order by 1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }

        String sqlText = "explain select a,z from t1";
        testQueryContains(sqlText, "IndexScan", methodWatcher, true);
    }

    @Test
    public void testShowPK() throws Exception {
        methodWatcher.execute("create table t2 (a1 int, b1 int, c1 int, primary key(a1, c1))");
        methodWatcher.execute("alter table t2 drop b1");
        try (ResultSet rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPRIMARYKEYS(null, 'DROPCOLUMNIT', 'T2', null)")) {
            rs.next();
            String column = rs.getString("COLUMN_NAME");
            assertEquals(column, "A1");
            int position = rs.getInt("KEY_SEQ");
            assertEquals(position, 1);

            rs.next();
            column = rs.getString("COLUMN_NAME");
            assertEquals(column, "C1");
            position = rs.getInt("KEY_SEQ");
            assertEquals(position, 2);
        }
    }

    @Test
    public void testMultiRowInsert() throws Exception {
        methodWatcher.execute("create table t3 (c1 int not null, c2 int not null, c3 int not null default 37, c4 int not null)");
        methodWatcher.execute("alter table t3 drop column c3");
        methodWatcher.execute("insert into t3 values (6,6,6), (7,7,7), (8,8,8), (9,9,9)");
        String expected =
                "C1 |C2 |C4 |\n" +
                "------------\n" +
                " 6 | 6 | 6 |\n" +
                " 7 | 7 | 7 |\n" +
                " 8 | 8 | 8 |\n" +
                " 9 | 9 | 9 |";
        try (ResultSet rs = methodWatcher.executeQuery("select * from t3 order by 1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDropPKSelect() throws Exception {
        methodWatcher.execute("create table t4(c1 int not null, c2 int not null, primary key (c1,c2))");
        methodWatcher.execute("alter table t4 drop column c2");
        methodWatcher.execute("insert into t4 values 1, 2");
        String expected = "C1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |";
        try (ResultSet rs = methodWatcher.executeQuery("select * from t4 order by 1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, actual);
        }
    }
}
