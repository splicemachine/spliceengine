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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;

public class Derby5652 extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  PROVIDER_PROPERTY = "derby.authentication.provider";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public Derby5652( String name ) { super(name); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite( "Derby5652" );

        Test    test = new Derby5652( "basicTest" );

        // turn off security manager so that we can change system properties
        test = SecurityManagerSetup.noSecurityManager( test );

        suite.addTest( test );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Trailing garbage after the credentials db name should produce a useful
     * error message instead of an assertion failure.
     * </p>
     */
    public  void    basicTest()  throws  Exception
    {
        // run the test in another process because it creates a zombie engine
        // which can't be killed. see db-5757.
        assertLaunchedJUnitTestMethod( getClass().getName() + ".badProperty", null );
    }
    
    /**
     * <p>
     * Trailing garbage after the credentials db name should produce a useful
     * error message.
     * </p>
     */
    public  void    badProperty()  throws  Exception
    {
        // bring down the engine in order to have a clean environment
        getTestConfiguration().shutdownEngine();

        // configure an illegal credentials db name--this one has an illegal trailing colon
        setSystemProperty( PROVIDER_PROPERTY, "NATIVE:db:" );

        // verify that you can't connect with this provider setting
        try {
            openUserConnection( "fooUser" );
        }
        catch (SQLException se)
        {
            // look for a login failure message. the detailed error is printed to
            // db.log and not percolated out of the Monitor.
            assertSQLState( "08004", se );
        }
    }
    
}
