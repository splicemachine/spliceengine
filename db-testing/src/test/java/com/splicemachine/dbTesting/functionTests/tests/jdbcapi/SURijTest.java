/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 *	Test that runs the SURTest_ij.sql script and compares the output 
 *	to SURTest_ij.out.
 */
public final class SURijTest extends ScriptTestCase {

	/**
	 * The test script
	 */
	private static final String[] TESTS = { "SURTest_ij" };
	
	
	/**
	 * Constructor that runs a single script.
	 * 
	 * @param script - the name of the script
	 */
	private SURijTest(String script) {
		super(script);
	}

	
	/**
	 * Return the suite that runs the script.
	 */
	public static Test suite() {

		TestSuite suite = new TestSuite("SURijTest");
		suite.addTest(TestConfiguration
				.clientServerDecorator(new CleanDatabaseTestSetup(
						new SURijTest(TESTS[0]))));
        suite.addTest(new CleanDatabaseTestSetup(
                        new SURijTest(TESTS[0])));
		return suite;
	}
}
