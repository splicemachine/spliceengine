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

package com.splicemachine.dbTesting.functionTests.tests.i18n;

import java.io.File;
import java.security.AccessController;
import java.util.Properties;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * LocalizedAttributeScriptTest runs the ij scripts (.sql file) 
 * LocalizedConnectionAttribute.sql and compares the output to a canon file in
 * the standard master package.
 * <BR>
 * Its suite() method returns the test as an instance of
 * this class for the specific script wrapped in a decorator that sets the
 * specific encoding properties, surrounded by a clean database decorator.
 * <BR>
 * It can also be used as a command line program
 *
 */
public final class LocalizedAttributeScriptTest extends ScriptTestCase {
    
    /**
     * Run LocalizedConnectionAttribute.sql 
     * <code>
     * example
     * java com.splicemachine.dbTesting.functionTests.tests.i18n.LocalizedAttributeScriptTest
     * </code>
     */
    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(getSuite());
    }

    /**
     * Return the suite that runs the Localized script.
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("LocalizedScripts");

        // Note that it doesn't really make sense to run with network 
        // server/client, as LocalizedConnectionAttributes.sql has a hardcoded
        // connect with protocol, so we can test connect 'as' with a us-ascii
        // character. So only run with embedded. 
        // Similarly, the script cannot run with JSR169, because the connect
        // statement automatically invokes DriverManager.
        if (JDBC.vmSupportsJSR169())
            return suite;
        TestSuite localizedTests = new TestSuite("LocalizedScripts:embedded");
        localizedTests.addTest(getSuite());
        Test local = TestConfiguration.singleUseDatabaseDecoratorNoShutdown(
            localizedTests);

        // add those client tests into the top-level suite.
        suite.addTest(local);

        return suite;
    }

    /*
     * A single JUnit test that runs a single Localized script, specifying the
     * desired input encoding.
     * Needs ISO-8859-1 encoding, or we get a syntax error on connecting to
     * the database with the u-umlaut in the 'as' clause.
     */
    private LocalizedAttributeScriptTest(String localizedTest){
        super(localizedTest, "ISO-8859-1");
    }

    /**
     * Return a suite of localized tests based on the 
     * script name. The test is surrounded in a decorator
     * that sets localization properties wrapped in a decorator
     * that cleans the database.
     */
    private static Test getSuite() {
        TestSuite suite = new TestSuite("localized scripts");
        Properties uiProps = new Properties();

        uiProps.put("derby.ui.locale","de_DE");
        uiProps.put("derby.ui.codeset","ISO-8859-1");
        suite.addTest(new SystemPropertyTestSetup(
                new LocalizedAttributeScriptTest("LocalizedConnectionAttribute"), uiProps));
        
        return getIJConfig(suite);
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of the extra database.
        // this also will get done if there are failures, and the database will
        // not be saved in the 'fail' directory.
        // We can't rely on an additionalDatabaseDecorator because 'detest'
        // is not just a logical, but a physical db name.
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                    removeDatabase("detest");
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