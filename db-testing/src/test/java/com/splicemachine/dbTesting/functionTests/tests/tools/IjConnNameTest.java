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
package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.File;

import java.security.AccessController;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;


import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;




/**
 * Test case for ijConnName.sql. 
 *
 */
public class IjConnNameTest extends ScriptTestCase {

    private static String test_script = "ijConnName";

    public IjConnNameTest(String name) {
        super(name, true);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("IjConnNameTest");

        // Test does not run on J2ME
        if (JDBC.vmSupportsJSR169())
            return suite;

        Properties props = new Properties();

        props.setProperty("ij.connection.connOne", "jdbc:splice:wombat;create=true");
        props.setProperty("ij.connection.connFour", "jdbc:splice:nevercreated");

        props.setProperty("ij.showNoConnectionsAtStart", "true");
        props.setProperty("ij.showNoCountForSelect", "true");

        Test test = new SystemPropertyTestSetup(new IjConnNameTest(test_script), props);
        //test = SecurityManagerSetup.noSecurityManager(test);
        test = new CleanDatabaseTestSetup(test);

        return getIJConfig(test);
    }

    public void tearDown() throws Exception {
        // attempt to get rid of the extra database.
        // this also will get done if there are failures, and the database will
        // not be saved in the 'fail' directory.
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                removeDatabase("lemming" );
                return null;
            }

            void removeDatabase(String dbName)
            {
                //TestConfiguration config = TestConfiguration.getCurrent();
                dbName = dbName.replace('/', File.separatorChar);
                String dsh = getSystemProperty("derby.system.home");
                if (dsh == null) {
                    fail("not implemented");
                } else {
                    dbName = dsh + File.separator + dbName;
                }
                removeDirectory(dbName);
            }

            void removeDirectory(String path)
            {
                final File dir = new File(path);
                removeDir(dir);
            }

            private void removeDir(File dir) {

                // Check if anything to do!
                // Database may not have been created.
                if (!dir.exists())
                    return;

                String[] list = dir.list();

                // Some JVMs return null for File.list() when the
                // directory is empty.
                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        File entry = new File(dir, list[i]);

                        if (entry.isDirectory()) {
                            removeDir(entry);
                        } else {
                            entry.delete();
                            //assertTrue(entry.getPath(), entry.delete());
                        }
                    }
                }
                dir.delete();
                //assertTrue(dir.getPath(), dir.delete());
            }
        });
        super.tearDown();
    }
}
