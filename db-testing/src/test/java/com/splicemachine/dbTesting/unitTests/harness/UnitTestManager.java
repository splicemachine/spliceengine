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

package com.splicemachine.dbTesting.unitTests.harness;

/**
 *
 * The UnitTestManager provides a mechanism for
 * registering subsystem tests and then invoking
 * them. It can produce an output report specifying
 * the results, the timing, and any output produced
 * by the tests. If the run fails, the invoker of
 * the tests should shut down the system.
 */
public interface UnitTestManager 
{
	public static final String MODULE = "com.splicemachine.dbTesting.unitTests.harness.UnitTestManager";
	
	/**
	 * Debug flag to allow the cloudscape system running the tests
	 * to run forever. By default test systems are killed 
	 * after an interval of T_Bomb.DEFAULT_BOMB_DELAY to avoid tests
	 * hanging.
	 */
	public static final String RUN_FOREVER = "RunForever";

	/**
	 * Debug flag to skip unit tests.
	 */
	public static final String SKIP_UNIT_TESTS = "SkipUnitTests";

	/**
	 * register an object that has the UnitTest interface,
	 * marking what type it has and its duration.
	 * Tests are run in the order they were registered.
	 * <p>
	 *
	 */
	public void registerTest(UnitTest objectToTest, String testName);
	

	/**
     * run the tests. Tests are run
     * in the order they were registered, filtered by type
     * and duration set for the unit test manager.
     */
	public boolean runTests();
	

	/**
	 * Convenience function to set the test type and duration
	 * for the UnitTestManager and then run the tests.
	 * <p>
	 * @see UnitTestConstants
	 */
	public boolean runTests(int testType, int testDuration);
	

	/**
     * the test duration is set.  This will be used when the
     * tests are run; no tests with duration more than
	  * specified will be run.
     */
	public void setTestDuration(int testDuration);
	

	/**
     * the test duration is set.  This will be used when the
     * tests are run; no tests with duration more than
	  * specified will be run.
     */
	public void setTestType(int testType);
	
	/**
     * specify whether performance statistics should be
	 * gathered when tests are run. The manager will collect
	 * the duration of each test, and will compare it to
	 * any previous runs it may have done of that test.
     */
	public void setPerformanceReportOn(boolean performanceReportOn);
	
}

