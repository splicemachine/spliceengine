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


import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test to test two threads doing select then delete of a row
 * using REPEATABLE_READ isolation level.  We should get a 
 * deadlock (SQLState 40001);
 *
 */
public class Derby3980DeadlockTest extends BaseJDBCTestCase {
    private final int THREAD_COUNT = 2;
    private LinkedList  listExceptions = new LinkedList();
    private Object syncObject = new Object();
    private int startedCount = 0;
    private String fprefix = "javacore";
    private static String TARGET_POLICY_FILE_NAME="derby3980deadlock.policy";
    
    public Derby3980DeadlockTest(String name) {
        super(name);
    }

    
    public void test3980Deadlock() {
        Thread [] t = new Thread[THREAD_COUNT];
        createThreads(t);
        waitForThreads(t);
        checkExceptions();        
    }
    
    
    /**
     * Check we have one deadlock exception.
     */
    private void checkExceptions() {        
        //Due to timing, you might see ERROR 40XL1: A lock could not be obtained
        //instead of ERROR 40001 (DERBY-3980)
        for( Iterator i = listExceptions.iterator(); i.hasNext(); ) {
            SQLException e = (SQLException) i.next();
            assertSQLState("40001",e);
        }
        assertEquals("Expected 1 exception, got" + listExceptions.size(),
                1,listExceptions.size());
    }
    /**
     * Decorate a test with SecurityManagerSetup, clientServersuite, and
     * SupportFilesSetup.
     * 
     * @return the decorated test
     */
    private static Test decorateTest() {
        String policyName = new Derby3980DeadlockTest("test").makePolicyName();
        Test test = TestConfiguration.clientServerSuite(Derby3980DeadlockTest.class);
        Properties diagProperties = new Properties();
        diagProperties.setProperty("derby.stream.error.extendedDiagSeverityLevel", "30000");
        diagProperties.setProperty("derby.infolog.append", "true");
        test = new SystemPropertyTestSetup(test, diagProperties, true);
     
        // Install a security manager using the initial policy file.
        test = new SecurityManagerSetup(test, policyName);

        // Copy over the policy file we want to use.
        String POLICY_FILE_NAME=
            "functionTests/tests/store/Derby3980DeadlockTest.policy";

        test = new SupportFilesSetup
        (
                test,
                null,
                new String[] { POLICY_FILE_NAME },
                null,
                new String[] { TARGET_POLICY_FILE_NAME}
        );
        return test;
    }
    /**
     * Generate the name of the local policy file
     * @return the name of the local testing policy file
     **/
    private String makePolicyName() {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            String  fileName = userDir + File.separator + 
            SupportFilesSetup.EXTINOUT + File.separator + TARGET_POLICY_FILE_NAME;
            File      file = new File( fileName );
            String  urlString = file.toURL().toExternalForm();

            return urlString;
        }
        catch (Exception e) {
            fail("Unexpected exception caught by " +
                    "makePolicyName(): " + e );
            return null;
        }
    }
    
    private void waitForThreads(Thread[] t) {
        for (int i = 0; i < THREAD_COUNT; i++)
        {   
            try {
                t[i].join();
            } catch (InterruptedException e){
               fail(
                        "FAIL - InterruptedException  thrown waiting for the threads");
            }
        }
        
    }

    private void createThreads(Thread[] t) {
        for (int i = 0; i < THREAD_COUNT; i++)
        {   
            t[i] = new Thread(new Runnable() {
                public void run() {threadWorker(); }

                private void threadWorker() {
                    Connection threadConnection = null;
                    
                    try {
                        synchronized (syncObject) {
                            
                            /* If a connection hasn't been opened for this thread, open one */
                            if (threadConnection == null){
                                threadConnection = openDefaultConnection();
                            }
                            
                            /* A new thread started, so we increment the counter */
                            startedCount++;
                            
                            /* Wake all the threads to run the check below */
                            syncObject.notifyAll();
                            
                            while (startedCount < THREAD_COUNT) {
                                syncObject.wait();
                            }
                        }          
                        Statement stmt = threadConnection.createStatement();
                    
                    threadConnection.setAutoCommit(false);
                    /* set isolation level to repeatable read */
                    threadConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    
                    ResultSet rs = stmt.executeQuery("select * from t where i = 456");
                    while (rs.next());
                    
                    //stmt.executeUpdate("update t set i = 456 where i = 456")
                    stmt.executeUpdate("delete from t  where i = 456");
                    threadConnection.commit();
                    } catch (Exception e) {
                        synchronized(syncObject){
                            listExceptions.add(e);
                        }
                    }

                    
                }},"Thread"+i);
            t[i].start();
        }
        
    }

    public static Test suite() {
    Test suite = decorateTest();
 
    return new  CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 5, 10)) {
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

    /**
     * Use tearDown to cleanup some of the diagnostic information files
     */
    protected void tearDown() throws Exception {
        String dsh = BaseTestCase.getSystemProperty("user.dir");
        try {
          File basedir = new File(dsh);
          String[] list = BaseTestCase.getFilesWith(basedir, fprefix);
          removeFiles(list);
        } catch (IllegalArgumentException e) {
            fail("open directory");
        }   
        
        super.tearDown();
    }
}

