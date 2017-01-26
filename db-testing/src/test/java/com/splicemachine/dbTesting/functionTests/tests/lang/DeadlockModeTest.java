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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.Decorator;
import com.splicemachine.dbTesting.junit.JDBC;

/**
 * This tests for deadlock which can occur if two threads get a 
 * row lock before getting a table lock on the same table.  This can
 * happen if the lock obtained by the insert, update or delete result set
 * is a smaller range than the table scan result set.  The insert, update or
 * delete result set lock is obtained first.  For example, if the insert, update
 * or delete result set obtain a row lock and then the table scan obtains a
 * table scan lock, deadlock can occur since two threads can obtain the row lock
 * and then both thread will want the same table lock.
 */
public class DeadlockModeTest extends BaseJDBCTestCase {

    /* Number of threads to use */
    private static final int THREAD_COUNT = 20;
    
    /* Object used to manage the thread synching */
    private Object syncObject = new Object();
    
    /* Amount of threads started so far */
    private int startedCount = 0;
    
    /* Exceptions thrown by threads (if any) */
    private LinkedList listExceptions = new LinkedList();
    
    /**
     * Creates a new instance of DeadlockModeTest
     * @param name identifier of the test
     */
	public DeadlockModeTest(String name) {
	    super(name);
	}
    
    /**
     * Tests for a deadlock on concurrent accesses to the database
     */
    public void testThreadsUpdatingTable() throws Exception {
        Thread [] t = new Thread[THREAD_COUNT];
        
        createThreads(t);
        waitForThreads(t);
        
        if ( !listExceptions.isEmpty() ) {
            fail("FAIL - " + listExceptions.size() + " threads threw exceptions");
        }
    }
    
    /**
     * This method creates THREAD_COUNT threads which will all try to
     * update the same table 
     */
    private void createThreads(Thread [] t) throws SQLException {
        for (int i = 0; i < THREAD_COUNT; i++)
        {
            final Connection c = openDefaultConnection();
            t[i] = new Thread(new Runnable() {
                public void run() {threadWorker(c); }});
            t[i].start();
        }
    }
    
    /**
     * This method waits for the threads to finish their execution.
     * This call will block the execution until signaled otherwise.
     */
    private void waitForThreads(Thread [] t) throws Exception {
        for (int i = 0; i < THREAD_COUNT; i++)
        {   
            try {
                t[i].join();
            } catch (InterruptedException e){
                assertNull(
                        "FAIL - Exception thrown waiting for the threads",
                        e);
            }
        }
    }
    
    
    /**
     * This method has the code that each thread will be running.
     * Since this will be running within different threads, we 
     * can not throw exceptions, therefore we make a "bogus"
     * assertNull(string message,Exception e) call.
     */
    private void threadWorker(Connection threadConnection) {
        try {
            synchronized (syncObject) {
                /* A new thread started, so we increment the counter */
                startedCount++;
                
                /* Wake all the threads to run the check below */
                syncObject.notifyAll();
                
                while (startedCount < THREAD_COUNT) {
                    syncObject.wait();
                }
            }          
            Statement stmt = threadConnection.createStatement();
            
            /* execute a query to load cache */
            stmt.executeUpdate("update t set i = 456 where i = 456");
            
            threadConnection.setAutoCommit(false);
            
            /* set isolation level to serializable */
            stmt.execute("set isolation serializable");
            
            for (int i = 0; i < 100 ; i++) {
                stmt.executeUpdate("update t set i = 456 where i = 456");
                threadConnection.commit();
            }

            threadConnection.close();
        } catch (Exception e) {
            synchronized(syncObject){
                listExceptions.add(e);
            }
        }
    }
    
    protected static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(DeadlockModeTest.class);
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4)) 
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                stmt.execute("create table t (i int)");
                
                stmt.executeUpdate("insert into t values (1956)");
        
                stmt.executeUpdate("insert into t values (180)");
        
                stmt.executeUpdate("insert into t values (3)");
            }
        };
    } 
    
    public static Test suite() {
        TestSuite suite = new TestSuite("DeadlockModeTest ");
        suite.addTest(
                baseSuite("DeadlockModeTest:embedded")
                );
        
        /* JSR169 does not have encryption support */
        if ( JDBC.vmSupportsJDBC3() ) {
            suite.addTest(Decorator.encryptedDatabase(
                    baseSuite("DeadlockModeTest:encrypted")));
        }

        return suite;        
    }
}
