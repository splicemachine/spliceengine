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
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Run an autoload test with the system property
 * jdbc.drivers being set. This test is only intended
 * to be run in its own vm as a single test run.
 * It inherits from TestCase so as to not have any
 * chance of loading DriverManager before setting
 * jdbc.drivers. 
 */
abstract class JDBCDriversPropertyTest extends TestCase {
    
    final static Test getSuite(String jdbcDrivers) throws Exception {
        
        TestSuite suite = new TestSuite("jdbc.drivers="+jdbcDrivers);
        
        System.setProperty("jdbc.drivers", jdbcDrivers);
        
        suite.addTest(getAutoLoadSuite());
            
        return suite;
    }
    
    /**
     * Load the class and run its suite method through reflection
     * so that DriverManger is not loaded indirectly before
     * jdbc.drivers is set.
     */
    private static Test getAutoLoadSuite()
       throws Exception
    {
        Class alt = Class.forName(
           "com.splicemachine.dbTesting.functionTests.tests.jdbcapi.AutoloadTest");
        
        Method suiteMethod = alt.getMethod("suite", null);
        
        return (Test) suiteMethod.invoke(null, null);
    }
    
    JDBCDriversPropertyTest() {
        super();
     }
}
