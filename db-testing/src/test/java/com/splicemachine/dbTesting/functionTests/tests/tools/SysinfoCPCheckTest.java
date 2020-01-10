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

package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.security.AccessController;
import java.util.Locale;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.Derby;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.LocaleTestSetup;

public class SysinfoCPCheckTest extends BaseJDBCTestCase {

    public SysinfoCPCheckTest(String name) { 
        super(name); 
    }

    private static boolean isClient = true;
    private static boolean isServer = true;
    
    public static Test suite() {

        if (!Derby.hasTools())
            return new TestSuite("empty: no tools support");
        
        // check to see if we have derbynet.jar or derbyclient.jar
        // before starting the security manager...
        if (!Derby.hasServer())
            isServer=false;
        if (!Derby.hasClient())
            isClient=false;

        Test suite = new TestSuite(SysinfoCPCheckTest.class, "Sysinfo ClassPath Checker");        
        return new LocaleTestSetup(suite, Locale.ENGLISH);
    }

    /**
     * Test sysinfo.testClassPathChecker()
     *
     * Tests sysinfo classpathtester
     * This test compares expected output strings; expected language is en_US
     * 
     */
    /**
     *  Test Classpath Checker output for 3 supported variations
     */
    public void testClassPathChecker() throws IOException {
        String Success = "SUCCESS: All Derby related classes found in class path.";
        // for testing the -cp with valid class
        String thisclass = "com.splicemachine.dbTesting.functionTests.tests.tools." +
        "SysinfoCPCheckTest.class";
        // initialize the array of arguments and expected return strings
        // The purpose of the values in the inner level is:
        // {0: argument to be passed with -cp,
        //  1: line number in the output to compare with,
        //  2: string to compare the above line with
        //  3: optional string to search for in addition to the above line
        //  4: a string/number to unravel the output
        final String[][] tstargs = {
                // empty string; should check all; what to check? Just check top
                // to ensure it recognizes it needs to check all.
                {null, "0", "Testing for presence of all Derby-related " +
                    "libraries; typically, only some are needed.", null},
                // incorrect syntax, or 'args' - should return usage
                {
                        "a",
                        "0",
                        "USAGE: java com.splicemachine.db.tools.sysinfo -cp ["
                                + " [ embedded ][ server ][ client] [ tools ]"
                                + " [ anyClass.class ] ]", null },
                {"embedded", "6", Success, "derby.jar"},
                {"server", "10", Success, "derbynet.jar"},
                {"tools", "6", Success, "derbytools.jar"},
                {"client", "6", Success, "derbyclient.jar"},
                {thisclass, "6", Success, "SysinfoCPCheckTest"},
                // neg tst, hope this doesn't exist
                {"nonexist.class", "6", "    (nonexist not found.)", null}
        };

        

        PrintStream out = System.out;

        int tst=0;
        for (tst=0; tst<tstargs.length ; tst++)
        {
            ByteArrayOutputStream rawBytes = getOutputStream();

            // First obtain the output for the sysinfo command
            PrintStream testOut = new PrintStream(rawBytes,
                    false);
            setSystemOut(testOut);
         
            if (!checkClientOrServer(tstargs[tst][0]))
                continue;

            // First command has only 1 arg, prevent NPE with if/else block 
            if (tstargs[tst][0] == null)
                com.splicemachine.db.tools.sysinfo.main(new String[] {"-cp"} );
            else
                com.splicemachine.db.tools.sysinfo.main(
                    new String[] {"-cp", tstargs[tst][0]} );

            setSystemOut(out);

            rawBytes.flush();
            rawBytes.close();

            byte[] testRawBytes = rawBytes.toByteArray();

            //System.out.println("cp command: -cp " + tstargs[tst][0]);

            String s = null;

            try {
                BufferedReader sysinfoOutput = new BufferedReader(
                    new InputStreamReader(
                        new ByteArrayInputStream(testRawBytes)));

                // evaluate the output
                // compare the sentence picked

                // first one is a bit different - is classpath dependent, so
                // we're not going to look through all lines.
                if (tstargs[tst][0]==null)
                {
                    s=sysinfoOutput.readLine();
                    assertEquals(tstargs[tst][2], s);
                    while (s != null)
                    {
                        s=sysinfoOutput.readLine();
                    }
                    continue;
                }

                if (!checkClientOrServer(tstargs[tst][0]))
                    continue;

                // get the appropriate line for the full line comparison
                int linenumber = Integer.parseInt(tstargs[tst][1]);

                boolean found = false;

                for (int i=0; i<linenumber; i++)
                {
                    s = sysinfoOutput.readLine();
                    if (tstargs[tst][3] != null)
                    {
                        // do the search for the optional string comparison
                        if (s.indexOf(tstargs[tst][3])>0)
                            found = true;
                    }
                }
                if (tstargs[tst][3] != null && !found)
                    fail ("did not find the string searched for: " + 
                         tstargs[tst][3] + " for command -cp: " + tstargs[tst][0]);

                // read the line to be compared
                s = sysinfoOutput.readLine();

                if (s == null)
                    fail("encountered unexpected null strings");
                else
                {
                    assertEquals(tstargs[tst][2], s);
                }

                // read one more line - should be the next command's sequence number
                s = sysinfoOutput.readLine();

                sysinfoOutput.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean checkClientOrServer(String kind)
    {
        if (kind == null)
            return true;
        // if there is no derbynet.jar, the syntax should still
        // work, but the comparisons will fail. So never mind.
        // JSR169 / J2ME does not support client or server
        if ((kind.equals("server") || kind.equals("client")) 
                && JDBC.vmSupportsJSR169())
            return false;

        if (kind.equals("server")) 
            return isServer;
        // same for derbyclient.jar
        if (kind.equals("client"))
            return isClient;
        return true;
    }

    /**
     * Need to capture System.out so that we can compare it.
     * @param out
     */
    private void setSystemOut(final PrintStream out)
    {
        AccessController.doPrivileged
        (new java.security.PrivilegedAction(){

            public Object run(){
                System.setOut(out);
                return null;
            }
        }
        );       
    }

    ByteArrayOutputStream getOutputStream() {
        return new ByteArrayOutputStream(20 * 1024);
    }
}
