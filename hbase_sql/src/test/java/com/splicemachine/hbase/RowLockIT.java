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

package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@Category({SerialTest.class})
public class RowLockIT {
    private static final String SCHEMA = RowLockIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A", spliceSchemaWatcher.schemaName,
            "(i int)");
    private static final SpliceTableWatcher B_TABLE = new SpliceTableWatcher("B", spliceSchemaWatcher.schemaName,
            "(i int primary key)");

    private static final String A_INIT = "INSERT INTO A VALUES 1,2,3,4,5";
    private static final String A_VALS = "INSERT INTO A select i+(select count(*) from a) from a";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(A_TABLE)
            .around(B_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.getStatement().executeUpdate(A_INIT); // 5
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS); // 10
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS); // 20
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS); // 40
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS); // 80
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(90);

    @Test
    public void testRowLockDoesntDeadlock() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        for (int step = 0; step < 1000; step += 100) {
            try {
                conn.createStatement().executeUpdate("insert into b \n" +
                        "select i+" + step + " from (\n" +
                        "select i from a --splice-properties useSpark=true\n" +
                        "union all\n" +
                        "select 81-i as i from a) a");
                fail("Statement didn't fail despite PK violation");
            } catch (SQLException e) {
                assertEquals("Unexpected error type", "23505", e.getSQLState());
            }
        }
        // ensure no rows are left on b
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from b");
        assertTrue(rs.next());

        // TODO fix SPLICE-1470
        // assertEquals("Rows inserted in b despite PK violations", 0, rs.getInt(1));
    }
}

