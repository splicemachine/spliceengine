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

package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.Matchers.lessThan;

public class GetNearestTransactionIT {

    private static final String schemaName = GetNearestTransactionIT.class.getSimpleName().toUpperCase();

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(schemaName);

    private static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a INT)");

    private static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn;
    private static TestConnection connACOff;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn = classWatcher.getOrCreateConnection();
        connACOff = classWatcher.createConnection();
        connACOff.setAutoCommit(false);
    }

    long resultOf(String query, TestConnection conn) throws SQLException {
        ResultSet rs = conn.query(query);
        Assert.assertTrue(rs.next());
        long result = rs.getLong(1);
        Assert.assertFalse(rs.wasNull());
        Assert.assertFalse(rs.next());
        return result;
    }

    @Test
    public void GetNearestTransactionBeforeFirstTransactionReturnsNull() throws Exception {
        ResultSet rs = conn.query("VALUES get_nearest_Transaction(TIMESTAMPADD(SQL_TSI_MONTH, -10, CURRENT_TIMESTAMP ))");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getLong(1));
        Assert.assertTrue(rs.wasNull());
        Assert.assertFalse(rs.next());
    }

    @Test
    public void GetNearestTransactionWorksCorrectlyWithAutoCommit() throws Exception {
        try {
            long result1 = resultOf("VALUES get_nearest_Transaction(CURRENT_TIMESTAMP)", connACOff);
            long result2 = resultOf("VALUES get_nearest_Transaction(CURRENT_TIMESTAMP)", connACOff);
            Assert.assertEquals(result1, result2);
        } finally {
            try {
                connACOff.execute("ROLLBACK");
            } catch (Exception e) { /*nothing*/ }
        }
    }

    @Test
    public void GetNearestTransactionWorksCorrectly() throws Exception {
        long result1 = resultOf("VALUES get_nearest_Transaction(CURRENT_TIMESTAMP)", conn);
        // some new transaction
        conn.execute(String.format("INSERT INTO %s.A VALUES 42", schemaName));
        long result2 = resultOf("VALUES get_nearest_Transaction(CURRENT_TIMESTAMP)", conn);
        Assert.assertThat(result1, lessThan(result2));
    }
}
