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

package com.splicemachine.dbTesting.unitTests.services;

import com.splicemachine.db.iapi.services.diag.Diagnosticable;
import com.splicemachine.db.iapi.services.diag.DiagnosticUtil;


import com.splicemachine.dbTesting.unitTests.harness.T_MultiIterations;
import com.splicemachine.dbTesting.unitTests.harness.T_Fail;
import com.splicemachine.db.iapi.reference.Property;

import java.util.Properties;

// DEBUGGING:

/**

  This T_Diagnosticable class provides a sample of how to use the "Diagnostic"
  facility.  The classes methods are built to be called by a "values" or
  a "call" statement from "ij".  Eventually there will be some sort of 
  diagnostic monitor which will be used to call the various "D_*" routines.

**/


public class T_Diagnosticable extends T_MultiIterations
{
    private static final String testService = "DiagnosticableTest";


    /* Constructors for This class: */

    /**
     * No arg Constructor.
     **/
    public T_Diagnosticable()
    {
    }

    /* Private/Protected methods of This class: */

    /**
     * Simple test of DiagnosticUtil interfaces.
     * <p>
     * Simple test of DiagnosticUtil.toDiagString() and 
     * DiagnosticUtil.findDiagnostic() interfaces.
     *
	 * @exception  T_Fail  If test fails for some reason.
     **/
    private void t_001()
        throws T_Fail
    {
        // Create object with also has a diagnostic interface:
        Object diag_obj = new T_DiagTestClass1("object with diag interface");

		// Create an object in a sub-class that doesn't have a D_ class, but
		// its super-class does.
		Object diagSubObj = new T_DiagTestClass1Sub("sub-class");

        // Create object with neither Diagnosticable:
        Object obj = new Long(5);

        // Test just getting a single string back, from each type of object.
        String          str          = null;
        String          expected_str = null;
        Diagnosticable  helper_class = null;

        // Here the string should come from the Diagnostic object's diag().
        str          = DiagnosticUtil.toDiagString(diag_obj);
        expected_str = "D_T_DiagTestClass1: object with diag interface";

        if (str.compareTo(expected_str) != 0)
        {
			throw T_Fail.testFailMsg(
                "DiagnosticUtil.toDiagString() failed, got: (" + str + 
                "), expected: (" + expected_str + ").");
        }


        // make sure right class was found.
      
        helper_class = DiagnosticUtil.findDiagnostic(diag_obj);
        
        if (!(helper_class instanceof D_T_DiagTestClass1))
            throw T_Fail.testFailMsg("Bad helper class lookup.");

        // make sure helper class gives right string.
        
        try
        {
            str = helper_class.diag();
        }
        catch (Throwable t)
        {
			throw T_Fail.testFailMsg(
                "Unexpected exception from helper_class.diag() call");
        }

        if (!str.equals(expected_str))
        {
			throw T_Fail.testFailMsg(
                "DiagnosticUtil.toDiagString() failed, got: (" + str + 
                "), expected: (" + expected_str + ").");
        }

		// make sure the Diagnostic class picks up a super-version of the D_ class
        str          = DiagnosticUtil.toDiagString(diagSubObj);
        expected_str = "D_T_DiagTestClass1: sub-class";
        if (!str.equals(expected_str))
        {
			throw T_Fail.testFailMsg(
                "DiagnosticUtil.toDiagString() failed, got: (" + str + 
                "), expected: (" + expected_str + ").");
        }
        
        // Here the string should just be the system's default toString.
        str          = DiagnosticUtil.toDiagString(obj);
        expected_str = "5";

        if (str.compareTo(expected_str) != 0)
        {
			throw T_Fail.testFailMsg(
                "DiagnosticUtil.toDiagString() failed, got: (" + str + 
                "), expected: (" + expected_str + ").");
        }

        // check that lookup for this class return correctly returns null,
        // since help class does not exist.
        helper_class = DiagnosticUtil.findDiagnostic(obj);

        if (helper_class != null)
            throw T_Fail.testFailMsg("Bad helper class - should be null.");
    }


    /* Public Methods of T_MultiIterations class: */

    /**
     * Routine one once per invocation of the test by the driver.
     * <p>
     * Do work that should only be done once, no matter how many times
     * runTests() may be executed.
     *
	 * @exception  T_Fail  Thrown on any error.
     **/
    protected void setupTest()
		throws T_Fail
    {
		// don't automatic boot this service if it gets left around
		if (startParams == null) {
			startParams = new Properties();
		}
		startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
		// remove the service directory to ensure a clean run
		startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());
    }

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
        return("com.splicemachine.db.iapi.services.diag.DiagnosticUtil");
	}

    /**
     * Driver routine for the btree secondary index tests.
     * <p>
     *
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
	protected void runTestSet() throws T_Fail
	{
        out.println("Executing " + testService + " test.");

        t_001();

        out.println("Finished Executing " + testService + " test.");
	}
}
