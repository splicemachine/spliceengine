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

import java.sql.Statement;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * ToolScripts runs ij tool scripts (.sql files) in the tool package
 * and compares the output to a canon file in the
 * standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * tool based ij scripts as tests.
 *
 */
public final class ToolScripts extends ScriptTestCase {

    /**
     * Tool scripts (.sql files) that run under Derby's client
     * and emebedded configurations. Tool tests are put in this category
     * if they are likely to have some testing of the protocol,
     * typically tests related to data types.
     *
     */
    private static final String[] CLIENT_AND_EMBEDDED_TESTS = {
        "ij4", "ij6", "ij7",
    };

    /**
     * Tool scripts (.sql files) that only run in embedded.
     */
    private static final String[] EMBEDDED_TESTS = {
        "showindex_embed",
    };

    /**
     * Tool scripts (.sql files) that only run in client.
     */
    private static final String[] CLIENT_TESTS = {
        "showindex_client",
    };

    /**
     * Tests that run in embedded and require JDBC3_TESTS
     * (ie. can not run on JSR169).
     */
    private static final String[] JDBC3_TESTS = {
    	"qualifiedIdentifiers", "URLCheck",
    };


    /**
     * Tests that run with authentication and SQL authorization on.
     */
    private static final String[][][] SQLAUTHORIZATION_TESTS = {
        {{"ij_show_roles_dbo"}, {"test_dbo", "donald"}, {"test_dbo"}},
        {{"ij_show_roles_usr"}, {"test_dbo", "donald"}, {"donald"}}
    };

    /**
     * Run a set of tool scripts (.sql files) passed in on the
     * command line. Note the .sql suffix must not be provided as
     * part of the script name.
     * <code>
     * example
     * java com.splicemachine.dbTesting.functionTests.tests.tool.ToolScripts case union
     * </code>
     */
    public static void main(String[] args)
        {
            junit.textui.TestRunner.run(getSuite(args));
        }

    /**
     * Return the suite that runs all the tool scripts.
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("ToolScripts");
        suite.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        suite.addTest(getSuite(EMBEDDED_TESTS));
        if (JDBC.vmSupportsJDBC3())
            suite.addTest(getSuite(JDBC3_TESTS));
        suite.addTest(getAuthorizationSuite(SQLAUTHORIZATION_TESTS));

        // Set up the scripts run with the network client
        TestSuite clientTests = new TestSuite("ToolScripts:client");
        clientTests.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        clientTests.addTest(getAuthorizationSuite(SQLAUTHORIZATION_TESTS));
        clientTests.addTest(getSuite(CLIENT_TESTS));
        Test client = TestConfiguration.clientServerDecorator(clientTests);

        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }

    /*
     * A single JUnit test that runs a single tool script.
     */
    private ToolScripts(String toolTest){
        super(toolTest);
    }

    private ToolScripts(String toolTest, String user){
        super(toolTest,
              null /* default input encoding */,
              null /* default output encoding */,
              user);
    }

    /**
     * Return a suite of tool tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
     */
    private static Test getSuite(String[] list) {
        TestSuite suite = new TestSuite("Tool scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
                new CleanDatabaseTestSetup(
                    new ToolScripts(list[i])));

        return getIJConfig(suite);
    }

    /**
     * Return a suite of tool tests from the list of script names. Each test is
     * surrounded in a decorator that cleans the database, and adds
     * authentication and authorization for each script.
     * @param list <ul><li>list[i][0][0]: script name,
     *                 <li>list[i][1]: users,
     *                 <li>list[i][2][0]: run-as-user
     *             </ul>
     */
    private static Test getAuthorizationSuite(String[][][] list) {
        TestSuite suite = new TestSuite("Tool scripts w/authorization");
        final String PWSUFFIX = "pwSuffix";

        for (int i = 0; i < list.length; i++) {
            Test clean;

            if (list[i][0][0].startsWith("ij_show_roles")) {
                clean = new CleanDatabaseTestSetup(
                    new ToolScripts(list[i][0][0], list[i][2][0])) {
                        protected void decorateSQL(Statement s)
                                throws SQLException {
                            s.execute("create role a");
                            s.execute("create role b");
                            s.execute("create role \"\"\"eve\"\"\"");
                            s.execute("create role publicrole");
                            s.execute("grant a to b");
                            s.execute("grant publicrole to public");
                            s.execute("grant b to donald");
                        }
                    };
            } else {
                clean = new CleanDatabaseTestSetup(
                    new ToolScripts(list[i][0][0], list[i][2][0]));
            }

            suite.addTest(
                TestConfiguration.sqlAuthorizationDecorator(
                    DatabasePropertyTestSetup.builtinAuthentication(
                        clean, list[i][1], PWSUFFIX)));
        }

        return getIJConfig(suite);
    }
}
