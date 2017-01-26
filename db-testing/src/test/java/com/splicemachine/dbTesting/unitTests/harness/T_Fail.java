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
	Exception used to throw for errors in a unit test.
*/
public class T_Fail extends Exception  {

	private Throwable nested;

	/**
	  Create a T_Fail exception which carries a message.

	  @param message An Id for an error message for this exception.
	  */
	private T_Fail(String message) {
		super(message);
	}

	/**
		return a T_Fail exception to indicate the configuration does
		not specify the module to test.

		@return The exception.
	*/
	public static T_Fail moduleToTestIdNotFound()
	{
		return new T_Fail("Test failed because the configuration does not include the MODULE_TO_TEST_IDENT attribute.");
	}

	/**
		return a T_Fail exception to indicate the configuration does
		not contain the module to test.

		@return The exception.
	*/
	public static T_Fail moduleToTestNotFound(String moduleToTest)
	{
		return new T_Fail("Test failed due to failure loading " + moduleToTest);
	}

	/**
	  return a T_Fail exception to indicate the test failed due
	  to an exception.

	  <P>Note: Since the Test Service catches all exceptions this
	  seems to be of limited value.

	  @return The exception.
	*/
	public static T_Fail exceptionFail(Throwable e)
	{
		T_Fail tf = new T_Fail("The test failed with an exception: " + e.toString());
		tf.nested = e;
		return tf;
	}

	/**
	  return a T_Fail exception to indicate the test failed.

	  @return the exception.
	  */
	public static T_Fail testFail()
	{
		return new T_Fail("The test failed");
	}

	/**
	  return a T_Fail exception which includes a user message indicating
	  why a test failed.

	  @return The exception.
	*/
	public static T_Fail testFailMsg(String message)
	{
		return new T_Fail("Test failed - " + message);
	}

	/**
	  Check a test condition. If it is false, throw a T_Fail exception.

	  @param mustBeTrue The condition.
	  @exception T_Fail A test failure exception
	  */
	public static final void T_ASSERT(boolean mustBeTrue)
		 throws T_Fail
	{
		if (!mustBeTrue)
			throw testFail();
	}

	/**
	  Check a test condition. If it is false, throw a T_Fail exception which
	  includes a message.

	  @param mustBeTrue The condition.
	  @param msg A message describing the failue.
	  @exception T_Fail A test failure exception
	  */
	public static final void T_ASSERT(boolean mustBeTrue,String msg)
		 throws T_Fail
	{
		if (!mustBeTrue)
			throw testFailMsg(msg);
	}
}
