/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test cases for compressing tables.
 */
public class CompressTableTest extends BaseJDBCTestCase {

    public CompressTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        // compress table is an embedded feature, no need to run network tests
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(CompressTableTest.class));
    }

    /**
     * Test that SYSCS_COMPRESS_TABLE and SYSCS_INPLACE_COMPRESS_TABLE work
     * when the table name contains a double quote. It used to raise a syntax
     * error. Fixed as part of DERBY-1062.
     */
    public void testCompressTableWithDoubleQuoteInName() throws SQLException {
        Statement s = createStatement();
        s.execute("create table app.\"abc\"\"def\" (x int)");
        s.execute("call syscs_util.syscs_compress_table('SPLICE','abc\"def',1)");
        s.execute("call syscs_util.syscs_inplace_compress_table('SPLICE'," +
                  "'abc\"def', 1, 1, 1)");
        s.execute("drop table app.\"abc\"\"def\"");
    }

    /**
     * Test that statement invalidation works when SYSCS_COMPRESS_TABLE calls
     * and other statements accessing the same table execute concurrently.
     * DERBY-4275.
     */
    public void testConcurrentInvalidation() throws Exception {
        Statement s = createStatement();
        s.execute("create table d4275(x int)");
        s.execute("insert into d4275 values 1");

        // Object used by the main thread to tell the helper thread to stop.
        // The helper thread stops once the list is non-empty.
        final List stop = Collections.synchronizedList(new ArrayList());

        // Holder for anything thrown by the run() method in the helper thread.
        final Throwable[] error = new Throwable[1];

        // Set up a helper thread that executes a query against the table
        // until the main thread tells it to stop.
        Connection c2 = openDefaultConnection();
        final PreparedStatement ps = c2.prepareStatement("select * from d4275");

        Thread t = new Thread() {
            public void run() {
                try {
                    while (stop.isEmpty()) {
                        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
                    }
                } catch (Throwable t) {
                    error[0] = t;
                }
            }
        };

        t.start();

        // Compress the table while a query is being executed against the
        // same table to force invalidation of the running statement. Since
        // the problem we try to reproduce is timing-dependent, do it 100
        // times to increase the chance of hitting the bug.
        try {
            for (int i = 0; i < 100; i++) {
                s.execute(
                    "call syscs_util.syscs_compress_table('SPLICE', 'D4275', 1)");
            }
        } finally {
            // We're done, so tell the helper thread to stop.
            stop.add(Boolean.TRUE);
        }

        t.join();

        // Before DERBY-4275, the helper thread used to fail with an error
        // saying the container was not found.
        if (error[0] != null) {
            fail("Helper thread failed", error[0]);
        }

        // Cleanup.
        ps.close();
        c2.close();
    }
}
