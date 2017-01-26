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

package com.splicemachine.derby.utils;

import java.sql.CallableStatement;
import java.sql.ResultSet;

import com.splicemachine.test.SerialTest;
import org.apache.commons.dbutils.DbUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Integration tests for TimestampAdmin.
 */
@Ignore("-sf- needs to be re-implemented in an architecture-independent way, but I don't want to let that stop merging")
public class TimestampAdminIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = TimestampAdminIT.class.getSimpleName().toUpperCase();
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("TEST1",CLASS_NAME,"(a int)");
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).
        around(spliceSchemaWatcher).around(spliceTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /*
    private List<List<String>> resultSetToList(ResultSet resultset) throws Exception {
        int numcols = resultset.getMetaData().getColumnCount();
        List<List<String>> result = new ArrayList<List<String>>();
	    while (resultset.next()) {
	        List<String> row = new ArrayList<String>(numcols);
	        for (int i = 1; i <= numcols; i++) {
	            row.add(resultset.getString(i));
	        }
	        result.add(row);
	    }
	    return result;
    }
    */
    
    /**
     * Tests SYSCS_GET_TIMESTAMP_GENERATOR_INFO system procedure.
     */
    @Test
    public void testGetTimestampGeneratorInfo() throws Exception {
    	String template = "call SYSCS_UTIL.SYSCS_GET_TIMESTAMP_GENERATOR_INFO()";
        CallableStatement cs = methodWatcher.prepareCall(template);
        ResultSet rs = cs.executeQuery();
        int rowCount = 0;
        while (rs.next()) {
        	rowCount++;
        	long num = rs.getLong(1);
            Assert.assertTrue("Unexpected number of timestamps", num > 0);
        }
        Assert.assertTrue(rowCount == 1);
        DbUtils.closeQuietly(rs);
    }

    /**
     * Tests SYSCS_GET_TIMESTAMP_REQUEST_INFO system procedure.
     */
    @Test
    public void testGetTimestampRequestInfo() throws Exception {
    	String template = "call SYSCS_UTIL.SYSCS_GET_TIMESTAMP_REQUEST_INFO()";
        CallableStatement cs = methodWatcher.prepareCall(template);
        ResultSet rs = cs.executeQuery();
        int rowCount = 0;
        while (rs.next()) {
        	rowCount++;
        	long num = rs.getLong(2);
            Assert.assertTrue("Unexpected number of requests", num > 0);
        }
        Assert.assertTrue(rowCount > 0);
        DbUtils.closeQuietly(rs);
    }

}
