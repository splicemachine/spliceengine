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

package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Network client .sql tests to run via ij.
 */
/**
 * NetScripts runs ij scripts (.sql files) in the derbynet package
 * and compares the output to a canon file in the standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * ij scripts as tests.
 *
 */
public final class NetIjTest extends ScriptTestCase {

    /**
     * scripts (.sql files) - only run in client.
     */
    private static final String[] CLIENT_TESTS = {
        "testclientij",
    };

    /**
     * Run a set of scripts (.sql files) passed in on the
     * command line. Note the .sql suffix must not be provided as
     * part of the script name.
     * <code>
     * example
     * java com.splicemachine.dbTesting.functionTests.tests.derbynet.NetIjTest case union
     * </code>
     */
    public static void main(String[] args)
        {
            junit.textui.TestRunner.run(getSuite(args));
        }

    /**
     * Return the suite that runs all the derbynet scripts.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("NetScripts");

        // Set up the scripts run with the network client
        TestSuite clientTests = new TestSuite("NetScripts:client");
        clientTests.addTest(getSuite(CLIENT_TESTS));

        int port = TestConfiguration.getCurrent().getPort();

        Properties prop = new Properties();
        prop.setProperty("ij.protocol",
                "jdbc:splice://localhost:"+port+"/");

        Test client = new SystemPropertyTestSetup(
                TestConfiguration.clientServerDecoratorWithPort(clientTests,port),
                prop);
                    
        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }

    /*
     * A single JUnit test that runs a single derbynet script.
     */
    private NetIjTest(String netTest){
        super(netTest,true);
    }

    /**
     * Return a suite of derbynet tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
     */
    private static Test getSuite(String[] list) {
        TestSuite suite = new TestSuite("Net scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
                new CleanDatabaseTestSetup(
                    new NetIjTest(list[i])));

        return getIJConfig(suite);
    }
}
