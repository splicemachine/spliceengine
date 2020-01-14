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

package com.splicemachine.dbTesting.functionTests.tests.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This class contains tests for correct loading (booting) of modules
 * and factories.
 */
public class ModuleLoadingTest extends BaseJDBCTestCase {
    public ModuleLoadingTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite ts = new TestSuite();

        // Run concurrentLoadingOfSortFactory in a separate database so that
        // the sort factory isn't already loaded.
        ts.addTest(TestConfiguration.singleUseDatabaseDecorator(
                new ModuleLoadingTest("concurrentLoadingOfSortFactory")));

        return ts;
    }

    /**
     * Test case for DERBY-2074. When multiple threads tried to load
     * ExternalSortFactory concurrently, we sometimes got a
     * NullPointerException.
     */
    public void concurrentLoadingOfSortFactory() throws Throwable {
        // number of concurrent threads
        final int numThreads = 10;

        // Helper object to make it easier to refer to ModuleLoadingTest.this
        // from within the nested Runnable class. Used for synchronization
        // between the threads.
        final Object me = this;

        // Flag that tells the threads whether they're allowed to start.
        final boolean[] go = new boolean[1];
        // Active threads count.
        final int[] activeThreads = new int[1];
        // List of exceptions/throwables thrown by the forked threads.
        final ArrayList exceptions = new ArrayList();

        Thread[] threads = new Thread[numThreads];

        // Start the threads.
        for (int i = 0; i < numThreads; i++) {
            final Connection c = openDefaultConnection();
            // Prepare a statement that ends up calling
            // DistinctScalarAggregateResultSet.loadSorter().
            final PreparedStatement ps = c.prepareStatement(
                    "select count(distinct tablename) from sys.systables");
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    try {
                        _run();
                    } catch (Throwable t) {
                        synchronized (me) {
                            exceptions.add(t);
                        }
                    }
                }
                private void _run() throws Exception {
                    synchronized (me) {
                        // Notify the main thread that we're ready to execute.
                        activeThreads[0]++;
                        me.notifyAll();

                        // Wait for the main thread to notify us that we
                        // should go ahead.
                        while (!go[0]) {
                            me.wait();
                        }
                    }
                    // executeQuery() below used to get occational NPEs before
                    // DERBY-2074.
                    JDBC.assertDrainResults(ps.executeQuery());
                    ps.close();
                    c.close();
                }
            });
            threads[i].start();
        }

        // We want all threads to execute the statement at the same time,
        // so wait for all threads to be ready before giving them the GO
        // signal.
        synchronized (me) {
            while (activeThreads[0] < numThreads && exceptions.isEmpty()) {
                me.wait();
            }

            // All threads are active, or at least one of the threads have
            // failed, so tell the threads to stop waiting.
            go[0] = true;
            me.notifyAll();
        }

        // The threads have been started, now wait for them to finish.
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        // At least one of the threads failed. Re-throw the first error
        // reported.
        if (!exceptions.isEmpty()) {
            throw (Throwable) exceptions.get(0);
        }
    }
}
