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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SparkFlushMissingRowsIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SparkFlushMissingRowsIT.class);

    public static final String CLASS_NAME = StressSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schema);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testMissingRows() throws Exception {
        Connection conn = methodWatcher.createConnection();
        conn.setAutoCommit(false);
        HBaseTestingUtility testingUtility = new HBaseTestingUtility(HConfiguration.unwrapDelegate());
        Admin admin = testingUtility.getAdmin();

        conn.createStatement().executeUpdate("CREATE TABLE A (A1 int, a2 int)");
        conn.createStatement().executeUpdate("INSERT INTO A VALUES (1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1),(1,1)");
        for (int i = 0; i < 10; i++) {
            conn.createStatement().executeUpdate("insert into a select * from a --splice-properties useSpark=false\n");
        }

        String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, "A", methodWatcher);
        final TableName tableName = TableName.valueOf("splice", conglomerateNumber);
        admin.flush(tableName);

        conn.createStatement().executeUpdate("UPDATE A SET A1 = 2");

        PreparedStatement ps = conn.prepareStatement("SELECT a2 FROM A --splice-properties useSpark=true, splits=1\n");
        try (ResultSet rs = ps.executeQuery()) {
            rs.next();
            int numberOfRows = 1;
            admin.flush(tableName);
            while (rs.next()) {
                numberOfRows++;
                assertNotNull("Failure at row: " + numberOfRows, rs.getObject(1));
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}

