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

package com.splicemachine.db.iapi.services.diag;

/**

The Diagnostic framework is meant to provide a way to include as much
diagnostic capability within the distributed release of the Derby
product without adversely affecting the runtime speed or foot print of
a running configuration that needs not use this information.

In order to decrease the class size of running objects diagnostic information
should be put in "helper" classes.  So to provide diagnostic capabiility
on the implementation of class Foo.java create a class D_Foo.java.  Class
D_Foo must implement the Diagnosticable interface.  

This class provide utility functions to get at the information provided by
the D_* helper class:
    findDiagnostic() - given and object "obj", get an instance of D_obj. 
    toDiagString()   - return the "best" diagnostic string available about
                       a given object.

**/

public class DiagnosticUtil
{
    /* Constructors for This class: */
    private DiagnosticUtil()
    {
    }

    /* Private/Protected methods of This class: */

    /**
     * Given an object return instance of the diagnostic object for this class.
     * <p>
     * Given an object this routine will determine the classname of the object
     * and then try to instantiate a new instance of the diagnostic object
     * for this class by prepending on "D_" to the last element of theclassname.
	   If no matching class is found then the same lookup is made on the super-class
	   of the object, looking all the way up the hierachy until a diagnostic class
	   is found.
	 * <BR>
	   This routine will call "init(ref)" on the new instance and then return the new instance.
     *
	 * @return A new instance of the diagnostic object for input object, or
     *         null if one could not be found for some reason.
     *
     * @param ref   The object which to build the diagnostic object for.
     **/
    public static Diagnosticable findDiagnostic(Object ref)
    {
        Class refClass = ref.getClass();

		for (;;) {
			try 
			{
				String className = refClass.getName();
				int lastDot = className.lastIndexOf('.') + 1;
				String          diagClassName = 
					className.substring(0, lastDot) + 
					"D_" + className.substring(lastDot);

				Class diagClass;
				
				try {
					diagClass = Class.forName(diagClassName);
				} catch (ClassNotFoundException cnfe) {

					// try the super-class of the object
					refClass = refClass.getSuperclass();
					if (refClass == null)
						return null;

					continue;
				}


				Diagnosticable diag_obj = (Diagnosticable) diagClass.newInstance();

				diag_obj.init(ref);

				return diag_obj;
			}
			catch (Exception e)
			{
				return null;
			}
		}
	}

    /**
     * Return a diagnostic string associated with an object.
     * <p>
     * A utility interface to use if you just want to print a single string 
     * that represents the object in question.  In following order this routine
     * will deliver the string to use:
     * 
     *     1) find diagnostic help class, and use class.diag()
     *     2) else just use class.toString()
     *
     * <p>
     *
	 * @return The string describing the class input.
     *
     * @param obj The object to print out.
     *
     **/
    public static String toDiagString(Object obj)
    {
        String ret_string = null;

		if (obj == null) return "null";
        
        try 
        {
            Diagnosticable diag = DiagnosticUtil.findDiagnostic(obj);
            if (diag != null)
                ret_string = diag.diag();
        }
        catch (Throwable t)
        {
            // do nothing, ret_string should still be null on error
        }

        if (ret_string == null)
        {
            ret_string = obj.toString();
        }

        return(ret_string);
    }

    /* Public Methods of This class: */
    /* Public Methods of XXXX class: */
}
