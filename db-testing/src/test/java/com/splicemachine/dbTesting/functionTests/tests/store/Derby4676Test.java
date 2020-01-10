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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Regression test for DERBY-4676.
 */
public class Derby4676Test extends BaseJDBCTestCase {
    /** List of {@code HelperThread}s used in the test. */
    private List threads;

    public Derby4676Test(String name) {
        super(name);
    }

    /** Create a suite of tests. */
    public static Test suite() {
        return TestConfiguration.defaultSuite(Derby4676Test.class);
    }

    /** Set up the test environment. */
    protected void setUp() {
        threads = new ArrayList();
    }

    /** Tear down the test environment. */
    protected void tearDown() throws Exception {
        super.tearDown();

        List localThreads = threads;
        threads = null;

        // First, wait for all threads to terminate and close all connections.
        for (int i = 0; i < localThreads.size(); i++) {
            HelperThread t = (HelperThread) localThreads.get(i);
            t.join();
            Connection c = t.conn;
            if (c != null && !c.isClosed()) {
                c.rollback();
                c.close();
            }
        }

        // Then check if any of the helper threads failed.
        for (int i = 0; i < localThreads.size(); i++) {
            HelperThread t = (HelperThread) localThreads.get(i);
            if (t.exception != null) {
                fail("Helper thread failed", t.exception);
            }
        }
    }

    /**
     * <p>
     * Regression test case for DERBY-4676. Before the fix, fetching a row by
     * its row location would sometimes fail with a NullPointerException if
     * the row was deleted while the fetch operation was waiting for a lock.
     * </p>
     */
    public void testConcurrentFetchAndDelete() throws Exception {
        // Create a table to use in the test. Note that we need to have a
        // non-covering index on the table so that the row location is fetched
        // from the index and used to look up the row in the heap. If the
        // index covers all the columns, we won't fetch the row location from
        // it and the bug won't be reproduced.
        Statement s = createStatement();
        s.execute("create table t(x int, y int)");
        s.execute("create index idx on t(x)");

        // Create a thread that repeatedly inserts and deletes a row.
        HelperThread thread = new HelperThread() {
            void body(Connection conn) throws Exception {
                Thread.sleep(1000); // Wait for the select loop to start so
                                    // that the insert/delete loop doesn't
                                    // complete before it has started.
                Statement s = conn.createStatement();
                for (int i = 0; i < 1000; i++) {
                    s.execute("insert into t values (1,2)");
                    s.execute("delete from t");
                }
                s.close();
            }
        };

        startThread(thread);

        // As long as the insert/delete thread is running, try to read the
        // rows of the table using the index. This used to cause intermittent
        // NullPointerExceptions.
        while (thread.isAlive()) {
            JDBC.assertDrainResults(s.executeQuery(
                "select * from t --db-properties index=idx"));
        }
    }

    /**
     * Helper class for running database operations in a separate thread and
     * in a separate transaction.
     */
    private abstract class HelperThread extends Thread {
        Exception exception;
        Connection conn;

        public void run() {
            try {
                conn = openDefaultConnection();
                body(conn);
            } catch (Exception ex) {
                exception = ex;
            }
        }

        abstract void body(Connection conn) throws Exception;
    }

    /**
     * Start a helper thread and register it for automatic clean-up in
     * {@link #tearDown()}.
     *
     * @param thread the helper thread to start
     */
    private void startThread(HelperThread thread) {
        thread.start();
        threads.add(thread);
    }
}
