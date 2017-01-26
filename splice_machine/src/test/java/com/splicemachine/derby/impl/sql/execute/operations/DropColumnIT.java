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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.sql.actions.index.CustomerTable;
import com.splicemachine.derby.test.framework.*;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/17/14
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class DropColumnIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(DropColumnIT.class);

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String SCHEMA_NAME = DropColumnIT.class.getSimpleName().toUpperCase();
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


}
