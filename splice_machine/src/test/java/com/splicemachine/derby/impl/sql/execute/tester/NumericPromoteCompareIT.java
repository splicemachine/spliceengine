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

package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the implicit promotion and cast logic as implemented
 * in BinaryComparisonOperatorNode. There is some indirect
 * coverage in NumericConstantsIT and DataTypeCorrectnessIT too.
 */
public class NumericPromoteCompareIT {

	// This test class was added as part of fix for DB-1001.
	
    private static final String CLASS_NAME = NumericPromoteCompareIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(new SpliceSchemaWatcher(CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/NumericPromoteCompareIT.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static String tableName1 = "customer";
    
    @Test
    public void testMultipleTypes() throws Exception {
    	// Compare INT to the rest
        assertCount(5, tableName1, "cust_id_int", "=", "cust_id_int");
        assertCount(5, tableName1, "cust_id_int", "=", "cust_id_sml");
        assertCount(4, tableName1, "cust_id_int", "=", "cust_id_dec");
        assertCount(4, tableName1, "cust_id_int", "=", "cust_id_num");

        // Compare SMALLINT to the rest
        assertCount(5, tableName1, "cust_id_sml", "=", "cust_id_int");
        assertCount(5, tableName1, "cust_id_sml", "=", "cust_id_sml");
        assertCount(4, tableName1, "cust_id_sml", "=", "cust_id_dec");
        assertCount(4, tableName1, "cust_id_sml", "=", "cust_id_num");

        // Compare DECIMAL to the rest
        assertCount(4, tableName1, "cust_id_dec", "=", "cust_id_int");
        assertCount(4, tableName1, "cust_id_dec", "=", "cust_id_sml");
        assertCount(5, tableName1, "cust_id_dec", "=", "cust_id_dec");
        assertCount(5, tableName1, "cust_id_dec", "=", "cust_id_num");

        // Compare NUMERIC (which is same as DECIMAL) to the rest
        assertCount(4, tableName1, "cust_id_num", "=", "cust_id_int");
        assertCount(4, tableName1, "cust_id_num", "=", "cust_id_sml");
        assertCount(5, tableName1, "cust_id_num", "=", "cust_id_dec");
        assertCount(5, tableName1, "cust_id_num", "=", "cust_id_num");
    }

    // The following were lifted form NumericConstantsIT.
    // Might be good to consolidate them.
    
    /**
     * Executes two queries:
     *
     * SELECT * FROM [table] WHERE [operandOne] [operator] [operandTwo]
     * AND
     * SELECT * FROM [table] WHERE [operandTwo] [operator] [operandOne]
     */
    private void assertCount(int expectedCount, String table, String operandOne, String operator, Object operandTwo) throws Exception {
        String SQL_TEMPLATE = "select * from %s where %s %s %s";
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandOne, operator, operandTwo));
        String operatorTwo = newOperator(operator);
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandTwo, operatorTwo, operandOne));
    }

    private void assertCount(int expectedCount, String sql) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(format("count mismatch for sql='%s'", sql), expectedCount, count(rs));
    }

    private void assertException(String sql, Class expectedException, String expectedMessage) throws Exception {
        try {
            methodWatcher.executeQuery(sql);
            fail();
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static int count(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    private static String newOperator(String operator) {
        if ("<".equals(operator.trim())) {
            return ">";
        }
        if (">".equals(operator.trim())) {
            return "<";
        }
        if ("<=".equals(operator.trim())) {
            return ">=";
        }
        if (">=".equals(operator.trim())) {
            return "<=";
        }
        return operator;
    }

}
