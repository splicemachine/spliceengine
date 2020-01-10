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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;


/**
 * Tests of <code>javax.sql.Driver</code> for JDBC40 and up.
 */

public class Driver40Test extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public Driver40Test(String name) { super( name ); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
        if (JDBC.vmSupportsJSR169())
        {
            return new TestSuite(
                "DriverTest40 tests java.sql.Driver, not supported with JSR169");
        }
        return TestConfiguration.defaultSuite(Driver40Test.class);
    }
   
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * <p>
     * Test new method added by JDBC 4.1.
     * </p>
     */
    public void test_jdbc4_1() throws Exception
    {
        Driver  driver = DriverManager.getDriver( getTestConfiguration().getJDBCClient().getUrlBase() );
        println( "Testing a " + driver.getClass().getName() );

        Wrapper41Driver wrapper = new Wrapper41Driver( driver );

        try {
            wrapper.getParentLogger();
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
    }

}
