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

package com.splicemachine.client.workday;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for Workday bugs using small (relatively tiny) set of reproducing data.
 *
 * @author Jeff Cunningham
 *         Date: 9/9/13
 */
public class WorkdayTinyIT extends SpliceUnitTest { 
    private static final String SCHEMA_NAME = WorkdayTinyIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    protected static OmsLogTable omsLogTableWatcher = new OmsLogTable(OmsLogTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory() + "/workday/omslog.tiny");
        }
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(omsLogTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    // ===============================================================================
    // Test Helpers
    // ===============================================================================

    private static void assertValuesEqual(List<List<String>> expectedRowVals, List<List<String>> actualRowVals) {
        // compare size
        // if we do this first, we don't have to worry about ArrayIndexOOB ex below
        Assert.assertEquals("Row sizes differ", expectedRowVals.size(), actualRowVals.size());
        // compare each row
        StringBuilder buf = new StringBuilder();
        int rowNum = 0;
        for (List<String> expectedRow : expectedRowVals) {
            List<String> actualRow = actualRowVals.get(rowNum);
            int colNum = 0;
            for (String expectedColVal : expectedRow) {
                if (! expectedColVal.equals(actualRow.get(colNum))) {
                    buf.append("Row ").append(rowNum+1).append(" Col ").append(colNum+1).append(" Expected: ")
                            .append(expectedColVal).append(" Actual: ").append(actualRow.get(colNum)).append('\n');
                }
                ++colNum;
            }
            ++rowNum;
        }
        if (buf.length() > 0) {
            Assert.fail("ResultSet comparison failed:\n" +buf.toString());
        }
    }

    private static List<List<String>> serializeResultSet(ResultSet rs) throws SQLException {
        List<List<String>> actualRowVals = new ArrayList<List<String>>();
        int nCols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<String> row = new ArrayList<String>(nCols);
            for (int i=1; i<=nCols; i++) {
                row.add(rs.getObject(i).toString());
            }
            if (! row.isEmpty()) {
                actualRowVals.add(row);
            }
        }
        return actualRowVals;
    }

    // ===============================================================================
    // Query Tests
    // ===============================================================================

    private static final String BUG_710_QUERY_33 =
            String.format("select a.swh_date from %s.%s a where a.swh_date = date('2012-07-16') {limit 5}",
                    SCHEMA_NAME, OmsLogTable.TABLE_NAME);
    private static final String BUG_712_QUERY_8 =
            String.format("select count(*), sum(duration) as total_duration from %s.%s where" +
                            " offload_count = 0 and system_user_id='39$177'",
                    SCHEMA_NAME, OmsLogTable.TABLE_NAME);
    private static final String BUG_713_QUERY_16 =
            String.format("select distinct month(swh_date), swh_env, count(*) from %s.%s group"+
                    " by month(swh_date), swh_env order by month(swh_date), swh_env", SCHEMA_NAME, OmsLogTable.TABLE_NAME);
    private static final String BUG_713_QUERY_16_ORDERBY =
            String.format("select distinct month(swh_date), swh_env, count(*) from %s.%s group"+
                    " by month(swh_date), swh_env order by month(swh_date), swh_env", SCHEMA_NAME, OmsLogTable.TABLE_NAME);

    @Test
    public void testBug710query33NoIndex() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(BUG_710_QUERY_33);
        Assert.assertEquals(5, resultSetSize(rs));
    }

    @Test
    public void testBug713query16OrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(BUG_713_QUERY_16);
        List<List<String>> noOrderByVals = serializeResultSet(rs);
        rs = methodWatcher.executeQuery(BUG_713_QUERY_16_ORDERBY);
        List<List<String>> orderByVals = serializeResultSet(rs);
        assertValuesEqual(noOrderByVals, orderByVals);
    }

    @Test
    public void testBug712query8AllIndexes() throws Exception {
        Connection connection = methodWatcher.createConnection();
        try {
            // create the indexes as in bug 712
            SpliceIndexWatcher.createIndex(connection, SCHEMA_NAME, OmsLogTable.TABLE_NAME,
                    OmsLogTable.INDEX_WHDATE_IDX, OmsLogTable.INDEX_WHDATE_IDX_DEF, false);
            SpliceIndexWatcher.createIndex(connection, SCHEMA_NAME, OmsLogTable.TABLE_NAME,
                    OmsLogTable.INDEX_SYSUSERID_IDX, OmsLogTable.INDEX_SYSUSERID_IDX_DEF, false);
            SpliceIndexWatcher.createIndex(connection, SCHEMA_NAME, OmsLogTable.TABLE_NAME,
                    OmsLogTable.INDEX_HTTPREQ_IDX, OmsLogTable.INDEX_HTTPREQ_IDX_DEF, false);
            SpliceIndexWatcher.createIndex(connection, SCHEMA_NAME, OmsLogTable.TABLE_NAME,
                    OmsLogTable.INDEX_HTTPRESP_IDX, OmsLogTable.INDEX_HTTPRESP_IDX_DEF, false);

            // exec the query
            ResultSet rs = methodWatcher.executeQuery(BUG_712_QUERY_8);
            List<List<String>> actualRowVals = serializeResultSet(rs);
            List<List<String>> expectedRowVals = new ArrayList<List<String>>();
            expectedRowVals.add(Arrays.asList("3", "933"));
            assertValuesEqual(expectedRowVals, actualRowVals);
        } finally {
						dropIndex(SCHEMA_NAME,OmsLogTable.INDEX_WHDATE_IDX);
            dropIndex(SCHEMA_NAME,OmsLogTable.INDEX_SYSUSERID_IDX);
            dropIndex(SCHEMA_NAME,OmsLogTable.INDEX_HTTPREQ_IDX);
            dropIndex(SCHEMA_NAME,OmsLogTable.INDEX_HTTPRESP_IDX);
        }
    }

    private void dropIndex(String schemaName, String tableName){
        SpliceIndexWatcher.executeDrop(methodWatcher.getOrCreateConnection(),schemaName,tableName);
    }

}
