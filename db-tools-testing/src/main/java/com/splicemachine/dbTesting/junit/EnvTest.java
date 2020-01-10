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
package com.splicemachine.dbTesting.junit;

import junit.framework.TestCase;

/**
 * Simple Junit "test" that runs a number of fixtures to
 * show the environment a test would run in.
 * A fixture changes its name based upon the return
 * of a method that checks for some environmental condition,
 * e.g. does this vm support JDBC 3.
 * Meant as a simple aid to help determine any environment problems.
 *
 */
public class EnvTest extends TestCase {
	
	public EnvTest(String name)
	{
		super(name);
	}
	/*
	** Tests of the JDBC.vmSupportsXXX to see which JDBC support is available.
	*/
	public void testJSR169() {
		setName(JDBC.vmSupportsJSR169() + "_vmSupportsJSR169()");
	}
	public void testJDBC3() {
		setName(JDBC.vmSupportsJDBC3() + "_vmSupportsJDBC3()");
	}
	public void testJDBC4() {
		setName(JDBC.vmSupportsJDBC4() + "_vmSupportsJDBC4()");
	}
	/*
	** Tests of the Derby.hasXXX to see which Derby code is
	** available for the tests.
	*/
	public void testHasServer() {
		setName(Derby.hasServer() + "_hasServer");
	}
	public void testHasClient() {
		setName(Derby.hasClient() + "_hasClient");
	}
	public void testHasEmbedded() {
		setName(Derby.hasEmbedded() + "_hasEmbedded");
	}
	public void testHasTools() {
		setName(Derby.hasTools() + "_hasTools");
	}
    /*
    ** XML related tests
    */
    public void testClasspathHasXalanAndJAXP() {
        setName(XML.classpathHasJAXP() + "_classpathHasJAXP");
    }
    public void testClasspathMeetsXMLReqs() {
        setName(XML.classpathMeetsXMLReqs() + "_classpathMeetsXMLReqs");
    }
}
