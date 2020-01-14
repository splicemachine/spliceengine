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

package com.splicemachine.dbTesting.unitTests.harness;

import com.splicemachine.db.iapi.services.property.PropertyUtil;

/**
	Abstract class which executes T_Generic. This splits the running
	of a test into two parts, the test setup and running the test.
	This allows the setup to be performed once, and then the
	test itself to be run for a number of iterations. The number
	iterations is set by the property db.unittests.iterations
	and defaults to 1.
	<P>
	Statistics are provided about each iteration in the error log. The statistics
	are time for each iteration, used and total memory changes per iteration.

	@see T_Generic
*/
public abstract class T_MultiIterations extends T_Generic
{
	protected T_MultiIterations()
	{
		super();
	}

	/*
	** methods required by T_Generic
	*/

	
	/**
	  Run the test. The test should raise an exception if it
	  fails. runTests should return if the tests pass.

	  @exception T_Fail Test code throws these
	  */
	protected void runTests() throws T_Fail {

		setupTest();

		int	iterations = 1;

		/*
		** The property name for the number of iterations is
		** db.className.iterations.  For example, if the test
		** class is db.com.package.to.test.T_Tester,
		** the property name is db.T_Tester.iterations.
		*/
		String myClass = this.getClass().getName();
		String noPackage = myClass.substring(myClass.lastIndexOf('.') + 1);
		String propertyName = "derby." + noPackage + ".iterations";

		String iter = PropertyUtil.getSystemProperty(propertyName);
		if (iter != null) {
			try {
				iterations = Integer.parseInt(iter);
			} catch (NumberFormatException nfe) {
				// leave at one
			}
			if (iterations <= 0)
				iterations = 1;
		}

		for (int i = 0; i < iterations; i++) {
			Runtime.getRuntime().gc();
			long btm = Runtime.getRuntime().totalMemory();
			long bfm = Runtime.getRuntime().freeMemory();
			long bum = btm - bfm;

			long start = System. currentTimeMillis();

			runTestSet();

			long end = System. currentTimeMillis();

			Runtime.getRuntime().gc();
			long atm = Runtime.getRuntime().totalMemory();
			long afm = Runtime.getRuntime().freeMemory();
			long aum = atm - afm;

			out.println("Iteration " + i + " took " + (end - start) + "ms");
			out.println("Total memory increased by " + (atm - btm) + " is " + atm);
			out.println("Used  memory increased by " + (aum - bum) + " is " + aum);
		}
	}

	/*
	**	  Abstract methods to implement for your test.
	*/

	/**
		Run once to set up the test.

        @exception T_Fail Test code throws these
	*/
	protected abstract void setupTest() throws T_Fail;

	/**
		Run once per-iteration to run the actual test.

        @exception T_Fail Test code throws these
	*/
	protected abstract void runTestSet() throws T_Fail;
}
