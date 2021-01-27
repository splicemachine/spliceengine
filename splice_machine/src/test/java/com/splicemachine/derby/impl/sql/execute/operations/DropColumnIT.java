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

import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;


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

    protected static CustomerTable customerTableWatcher = new CustomerTable(CustomerTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/customer.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static String tableName = "TEST_TABLE";
    private static String indexName = "TEST_IDEXN";

    private static String tableWithConstraints = "TABLE_WITH_CONSTRAINTS";
    private static String tableWithConstraints2 = "TABLE_WITH_CONSTRAINTS2";

    @BeforeClass
    public static void prepare() throws Exception {
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int, d int, e int, f int, g int)", tableName))
                .withRows(rows(row(1,2,3,4,5,6,7)))
                .withInsert(format("insert into %s values (?,?,?,?,?,?,?)", tableName))
                .withIndex(format("create index %s on %s(c,f)", indexName, tableName)).create();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int, d int, primary key(c), unique(b))", tableWithConstraints))
                .withRows(rows(row(1,2,3,4)))
                .withInsert(format("insert into %s values (?,?,?,?)", tableWithConstraints))
                .create();
        new TableCreator(conn).withCreate(format("create table %s (a int, b int, c int references(a), d int)", tableWithConstraints2))
                .withRows(rows(row(1,2,3,4)))
                .withInsert(format("insert into %s values (?,?,?,?)", tableWithConstraints2))
                .create();
    }

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
    public SpliceWatcher methodWatcher = new SpliceWatcher();

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
}
