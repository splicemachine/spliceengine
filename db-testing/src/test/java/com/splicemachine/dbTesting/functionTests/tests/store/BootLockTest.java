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

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;

import com.splicemachine.db.iapi.store.raw.data.DataFactory;

/**
 * Testing file locks that prevent Derby "double boot" a.k.a "dual boot",
 * i.e. two VMs booting a database concurrently, a disaster scenario.
 * <p/>
 * For phoneME, test that the property {@code
 * db.database.forceDatabaseLock} protects us.
 * <p/>
 * FIXME: If DERBY-4646 is fixed, the special handling for phoneME
 * should be removed.
 */

public class BootLockTest extends BaseJDBCTestCase {

    private final static String dbName = "BootLockTestDB";
    private final static String dbDir = DEFAULT_DB_DIR + File.separator + dbName;
    public static String minionCompleteFileName = BootLockTest.dbDir + 
        File.separator + "minionComplete";
    private final static String dbLockFile = dbDir + File.separator +
    DataFactory.DB_LOCKFILE_NAME;
    private final static String dbExLockFile = dbDir + File.separator +
    DataFactory.DB_EX_LOCKFILE_NAME;
    private final static String servicePropertiesFileName = dbDir + File.separator + "service.properties";
    
    private static String[] cmd = new String[]{
        "com.splicemachine.dbTesting.functionTests.tests.store.BootLockMinion",
        dbDir,
        ""
    };

    private final static String DATA_MULTIPLE_JBMS_ON_DB = "XSDB6";
    private final static String DATA_MULTIPLE_JBMS_FORCE_LOCK = "XSDB8";
    // Ten minutes should hopefully be enough!
    public static final int MINION_WAIT_MAX_MILLIS = 600000;

    /**
     * Constructor
     *
     * @param name
     */
    public BootLockTest(String name) {
        super(name);
    }

    /**
     * Creates a suite.
     *
     * @return The test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("BootLockTest");
        suite.addTest(decorateTest());
        return suite;
    }


    /**
     * Decorate test with singleUseDatabaseDecorator and noSecurityManager.
     *
     * @return the decorated test
     */
    private static Test decorateTest() {

        Test test = new TestSuite(BootLockTest.class);

        if (JDBC.vmSupportsJSR169() && !isJ9Platform()) {
            // PhoneME requires forceDatabaseLock
            Properties props = new Properties();
            props.setProperty("derby.database.forceDatabaseLock", "true");
            test = new SystemPropertyTestSetup(test, props, true);
        }

        test = TestConfiguration.singleUseDatabaseDecorator(test, dbName);

        test = SecurityManagerSetup.noSecurityManager(test);

        return test;
    }


    public void testBootLock() throws Exception {

        Process p = null;

        p = execJavaCmd(cmd);
        waitForMinionBoot(p,MINION_WAIT_MAX_MILLIS);

        // We now know minion has booted

        try {
            Connection c = getConnection();
            fail("Dual boot not detected: check BootLockMinion.log");
        } catch (SQLException e) {
            if (JDBC.vmSupportsJSR169() && !isJ9Platform()) {
                // For PhoneME force database lock required
                assertSQLState(
                        "Dual boot not detected: check BootLockMinion.log",
                        DATA_MULTIPLE_JBMS_FORCE_LOCK,
                        e);
            } else {
                assertSQLState(
                        "Dual boot not detected: check BootLockMinion.log",
                        DATA_MULTIPLE_JBMS_ON_DB,
                        e);
            }
        }
        finally {
            if (p!= null) {
                p.destroy();
                p.waitFor();
            }
        }
        // Since all went OK, no need to keep the minion log file.
        File minionLog = new File("BootLockMinion.log");
        assertTrue(minionLog.delete());

        if (JDBC.vmSupportsJSR169()) {
            // Delete lock files so JUnit machinery can clean up the
            // one-off database without further warnings on System.err
            // (phoneMe).
            File db_lockfile_name = new File(dbLockFile);                    

            File db_ex_lockfile_name = new File(dbExLockFile);                    

            db_lockfile_name.delete();
            db_ex_lockfile_name.delete();
        }
    }

    private void waitForMinionBoot(Process p, int waitmillis) throws InterruptedException {
        boolean minionComplete;
        int minionExitValue;
        StringBuffer failmsg = new StringBuffer();
        // boolean set to true once we find the  lock file
        File lockFile = new File(dbLockFile);
        File servicePropertiesFile = new File(servicePropertiesFileName);
        // Attempt to catch any errors happening in minion for better test
        // diagnosis.
        BufferedReader minionSysErr = new BufferedReader(
            new InputStreamReader(p.getErrorStream()));
        String minionErrLine= null ;
        File checkFile = new File(minionCompleteFileName);
        do {
            if (checkFile.exists()) { 
                //The checkFile was created by BootLockMinion when we were
                //sure it was finished with creating the database and making 
                //the connection. It will get cleaned up with the database.
                return;
            }
            // otherwise sleep for a second and try again
            waitmillis -= 1000;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }
        } while (waitmillis > 0);
        
        // If we got here, the database did not boot. Try to print the error.
        failmsg.append(
                "Minion did not start or boot db in " +
                (MINION_WAIT_MAX_MILLIS/1000) +
                " seconds.\n");                
        try {
            minionExitValue = p.exitValue();
            minionComplete =true;
            failmsg.append("exitValue = " + minionExitValue);
        }catch (IllegalThreadStateException e )
        {
            // got exception on exitValue.
            // still running ..
            minionComplete=false;
        }
        // If the process exited try to print why.
        if (minionComplete) {
            failmsg.append("----Process exited. Minion's stderr:\n");
            do {
                try {
                    minionErrLine = minionSysErr.readLine();
                } catch (Exception ioe) {
                    // may not always work, so just bail out.
                    failmsg.append("could not read minion's stderr");
                }

                if (minionErrLine != null) {
                    failmsg.append(minionErrLine);
                }
            } while (minionErrLine != null);

            failmsg.append("\n----Minion's stderr ended");
        }
        
        p.destroy();
        p.waitFor();
        fail(failmsg.toString());
    }

}
