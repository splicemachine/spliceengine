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
package com.splicemachine.dbTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;

import junit.framework.Test;


import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test to test two threads doing staggered select then update of a row
 * Staggered selects should not starve the update.
 * 
 */
public class LiveLockTest extends BaseJDBCTestCase {
    private LinkedList listExceptions = new LinkedList();

    private Object syncObject = new Object();
    private boolean updateDone = false;

    public LiveLockTest(String name) {
        super(name);
    }

    /**
     * Start three threads. Two doing staggered selets and a third trying 
     * to do an update.  The update should not be starved by the staggered
     * selects.
     * @throws SQLException
     */
    public void testLiveLock() throws SQLException {

        Thread[] t = createThreads();
        waitForThreads(t);
        checkExceptions();
    }

    /**
     * There should be no exceptions. The update should have gotten through
     * 
     * @throws SQLException
     *             if any occurred
     */
    private void checkExceptions() throws SQLException {

        for (Iterator i = listExceptions.iterator(); i.hasNext();) {
            SQLException e = (SQLException) i.next();
            throw e;
        }

    }

    private void waitForThreads(Thread[] t) {
        for (int i = 0; i < t.length; i++) {
            try {
                t[i].join();
            } catch (InterruptedException e) {
                fail("FAIL - InterruptedException  thrown waiting for the threads");
            }
        }

    }

    private Thread[] createThreads() {
        Thread[] t = new Thread[3];
        // First select thread will start right away. Doing iterative
        // selects
        t[0] = new Thread(new Runnable() {
            public void run() {
                selectWorker(0);
            }
        }, "SelectThread1");
        // Second select will wait 1 second before it starts. so
        // selects will alternately be aquiring share locks.
        t[1] = new Thread(new Runnable() {
            public void run() {
                selectWorker(1000);
            }
        }, "SelectThread2");
        // Update thread waits 2 seconds to start to make sure both
        // selects have locks. It should not be starved.
        t[2] = new Thread(new Runnable() {
            public void run() {
                updateWorker();
            }
        }, "UpdateThread");
        t[0].start();
        t[1].start();
        t[2].start();
        return t;
    }

    private void selectWorker(int delay) {
        Connection threadConnection = null;

        try {
            if (delay > 0)
                Thread.sleep(delay);
            threadConnection = openDefaultConnection();
            Statement stmt = threadConnection.createStatement();

            threadConnection.setAutoCommit(false);
            /* set isolation level to repeatable read */
            threadConnection
                    .setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            // While our update is still not done, keep on doing the
            // staggered select.
            // Let's stop after 10 selects just in case the test fails 
            // and the update doesn't get through. We would have gotten
            // a lock timeout by then, so we will get a test failure.
            // We don't want it to run forever if live lock ever breaks.
            int tries = 0;
            while (!isUpdateDone()) {
                ResultSet rs = stmt.executeQuery("select * from t");
                while (rs.next())
                    ;
                Thread.sleep(4000);
                threadConnection.commit();
                tries++;
                if (tries == 10)
                    fail("Update did not occur after 10 selects");
            }
        } catch (Exception e) {
            synchronized (syncObject) {
                listExceptions.add(e);
            }
        }
    }

    private void updateWorker() {
        Connection threadConnection = null;
        try {
            Thread.sleep(2000);
            threadConnection = openDefaultConnection();
            Statement stmt = threadConnection.createStatement();

            stmt.executeUpdate("UPDATE T SET I = 456 where I = 456");
            synchronized (syncObject) {
                setUpdateDone(true);
                threadConnection.commit();
            }
        } catch (Exception e) {
            synchronized (syncObject) {
                listExceptions.add(e);
            }
        }
    }

    public static Test suite() {

        Test suite = TestConfiguration
                .embeddedSuite(LiveLockTest.class);
        return new CleanDatabaseTestSetup(DatabasePropertyTestSetup
                .setLockTimeouts(suite, 1, 10)) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {
                s.executeUpdate("CREATE TABLE T (I INT)");
                s.executeUpdate("INSERT INTO T VALUES(456)");
            }

        };

    }

    public boolean isUpdateDone() {
        synchronized (syncObject) {
            return updateDone;
        }
    }

    public void setUpdateDone(boolean updateDone) {
        synchronized (syncObject) {
            this.updateDone = updateDone;
        }
    }

}
