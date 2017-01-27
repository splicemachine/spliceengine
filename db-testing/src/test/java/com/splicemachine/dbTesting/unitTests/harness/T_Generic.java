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

import com.splicemachine.db.iapi.services.monitor.ModuleControl;

import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.Properties;

/**
	Abstract class which executes a unit test.

	<P>To write a test,	extend this class with a class which implements the two
	abstract methods:
<UL>	
    <LI>runTests
	<LI>setUp
</UL>
	@see UnitTest
	@see ModuleControl
*/
public abstract class T_Generic implements UnitTest, ModuleControl
{
	/**
	  The unqualified name for the module to test. This is set by the generic
	  code.
	  */
	protected String shortModuleToTestName;

	/**
	  The start parameters for your test. This is set by generic code.
	  */
	protected Properties startParams;

	/**
	  The HeaderPrintWriter for test output. This is set by the
	  generic code.
	  */
	protected HeaderPrintWriter out;

	protected T_Generic()
	{
	}

	/*
	** Public methods of ModuleControl
	*/

	/**
	  ModuleControl.start
	  
	  @see ModuleControl#boot
	  @exception StandardException Module cannot be started.
	  */
	public void boot(boolean create, Properties startParams)
		 throws StandardException
	{
		shortModuleToTestName =
			getModuleToTestProtocolName()
			.substring(getModuleToTestProtocolName().lastIndexOf('.')+1);

		this.startParams = startParams;
	}

	/**
	  ModuleControl.stop
	  
	  @see ModuleControl#stop
	  */
	public void stop() {
	}

	/*
	** Public methods of UnitTest
	*/
	/**
	  UnitTest.Execute
	  
	  @see UnitTest#Execute
	  */
	public boolean Execute(HeaderPrintWriter out)
	{
		this.out = out;

		String myClass = this.getClass().getName();
		String testName = myClass.substring(myClass.lastIndexOf('.') + 1);

		System.out.println("-- Unit Test " + testName + " starting");

		try
		{
			runTests();
		}
		
		catch (Throwable t)
		{
			FAIL(t.toString());
			t.printStackTrace(out.getPrintWriter());
			return false;
		}

		System.out.println("-- Unit Test " + testName + " finished");

		return true;
	}

	/**
	  UnitTest.UnitTestDuration
	  
	  @return UnitTestConstants.DURATION_MICRO
	  @see UnitTest#UnitTestDuration
	  @see UnitTestConstants
	  */
	public int UnitTestDuration() {
		return UnitTestConstants.DURATION_MICRO;
	}

	/**
	  UnitTest.UnitTestType
	  
	  @return UnitTestConstants.TYPE_COMMON
	  @see UnitTest#UnitTestType
	  @see UnitTestConstants
	  */
	public int UnitTestType() {
		return UnitTestConstants.TYPE_COMMON;
	}

	/**
	  Emit a message indicating why the test failed.

	  RESOLVE: Should this be localized?

	  @param msg the message.
	  @return false
	*/
	protected boolean FAIL(String msg) {
		out.println("[" + Thread.currentThread().getName() + "] FAIL - " + msg);
		return false;
	}

	/**
	  Emit a message saying the test passed.
	  You may use this to emit messages indicating individual test cases
	  within a unit test passed.

	  <P>RESOLVE:Localize this.
	  @param testName the test which passed.
	  @return true
	  */
	protected boolean PASS(String testName) {
		out.println("[" + Thread.currentThread().getName() + "] Pass - "+shortModuleToTestName +" " + testName);
		return true;
	}

	/**
		Emit a message during a unit test run, indent the message
		to allow the PASS/FAIL messages to stand out.
	*/
	public void REPORT(String msg) {
		out.println("[" + Thread.currentThread().getName() + "]     " + msg);
	}

	
	/**
	  Abstract methods to implement for your test.
	  */
	
	/**
	  Run the test. The test should raise an exception if it
	  fails. runTests should return if the tests pass.

	  @exception Exception Test code throws these
	  */
	protected abstract void runTests() throws Exception;

	/**
	  Get the name of the protocol for the module to test.
	  This is the 'factory.MODULE' variable.
	  
	  'moduleName' to the name of the module to test. 

	  */
	protected abstract String getModuleToTestProtocolName();
}
