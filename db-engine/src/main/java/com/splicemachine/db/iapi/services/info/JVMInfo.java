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

package com.splicemachine.db.iapi.services.info;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

// As an exception to the rule we import SanityManager from the shared package
// here, because the JVMInfo class is included in both db.jar and
// derbyclient.jar. Pulling in the class from the shared package allows us to
// unseal the shared package only (leaving iapi.services.sanity sealed).
import com.splicemachine.db.shared.common.sanity.SanityManager;

/**
	This class is used to determine which Java specification Derby will run at.
    For a useful discussion of how this class is used, please see DERBY-3176.
 */
public abstract class JVMInfo
{
	/**
		The JVM's runtime environment.
		<UL>
		<LI> 1 - not used was JDK 1.1
		<LI> 2 - not used, was for JDK 1.2 and 1.3
		<LI> 4 - J2SE_14 - JDK 1.4.0 or 1.4.1
		<LI> 5 - J2SE_142 - JDK 1.4.2
		<LI> 6 - J2SE_15 - JDK 1.5
		<LI> 7 - J2SE_16 - JDK 1.6
        <LI> 8 - J2SE_17 - JDK 1.7
        <LI> 9 - J2SE_18 - JDK 1.8
		</UL>
	*/
	public static final int JDK_ID;

	public static final int J2SE_14 = 4;
	public static final int J2SE_142 = 5;
	public static final int J2SE_15 = 6; // aka J2SE 5.0
	public static final int J2SE_16 = 7; // Java SE 6, not J2SE
    public static final int J2SE_17 = 8; // Java SE 7
    public static final int J2SE_18 = 9;

	public static final boolean J2ME;

	static 
	{
		int id;

		//
		// If the property java.specification.version is set, then try to parse
		// that.  Anything we don't recognize, default to Java 2 platform
		// because java.specification.version is a property that is introduced
		// in Java 2.  We hope that JVM vendors don't implement Java 1 and
		// set a Java 2 system property.
		// 
		// Otherwise, see if we recognize what is set in java.version.
		// If we don't recognize that, or if the property is not set, assume
		// version 1.4.
		//
		String javaVersion;
		String javaSpec;
		boolean isJ2ME;

		try {
			javaSpec = System.getProperty("java.specification.name");
		} catch (SecurityException se) {
			// some vms do not know about this property so they
			// throw a security exception when access is restricted.
			javaSpec = null;
		}

		try {
			javaVersion = System.getProperty("java.specification.version", "1.4");

		} catch (SecurityException se) {
			// some vms do not know about this property so they
			// throw a security exception when access is restricted.
			javaVersion = "1.4";
		}

		if (javaSpec != null &&
            (
             javaSpec.startsWith("J2ME") || // recognize IBM WCTME
             javaSpec.startsWith("CDC")  || // Oracle Java ME Embedded Client
             (
              (javaSpec.contains("Profile")) && // recognize phoneME
              (javaSpec.contains("Specification"))
             )
            )
            )
		{
			id = J2SE_14;
			isJ2ME = true;
		}
		else
		{
			// J2SE/J2EE
			isJ2ME = false;

			if (javaVersion.equals("1.4"))
			{
				String vmVersion = System.getProperty("java.version", "1.4.0");

				if (JVMInfo.vmCheck(vmVersion, "1.4.0") || JVMInfo.vmCheck(vmVersion, "1.4.1"))
					id = J2SE_14;
				else
					id = J2SE_142;
			}
			else if (javaVersion.equals("1.5"))
			{
				id = J2SE_15;
			}
			else if (javaVersion.equals("1.6"))
			{
				id = J2SE_16;
			}
            else if (javaVersion.equals("1.7"))
            {
                id = J2SE_17;
            }
            else if (javaVersion.equals("1.8")) {
                id = J2SE_18;
            }
			else
			{
				// aussme our lowest support unless the java spec
				// is greater than our highest level.
				id = J2SE_14;

				try {

                    if (Float.parseFloat(javaVersion) > 1.8f)
                        id = J2SE_18;
				} catch (NumberFormatException ignored) {
				}
			}
		}

		JDK_ID = id;
		J2ME = isJ2ME;
	}

	/**
		Check the vmVersion against a speciifc value.
		Sun jvms are of the form
	*/
	private static boolean vmCheck(String vmVersion, String id)
	{
		return vmVersion.equals(id) || vmVersion.startsWith(id + "_");
	}

	/**
		Return Derby's understanding of the virtual machine's environment.
	*/
	public static String derbyVMLevel()
	{
		switch (JDK_ID)
		{
		case J2SE_14: return J2ME ? "J2ME - JDBC for CDC/FP 1.1" : "J2SE 1.4 - JDBC 3.0";
		case J2SE_142: return "J2SE 1.4.2 - JDBC 3.0";
		case J2SE_15: return "J2SE 5.0 - JDBC 3.0";
        case J2SE_16: return "Java SE 6 - JDBC 4.1";
        case J2SE_17: return "Java SE 7 - JDBC 4.1";
        case J2SE_18: return "Java SE 8 - JDBC 4.1";
		default: return "?-?";
		}
	}

    /**
     * Get system property.
     *
     * @param name name of the property
     */
    private static String getSystemProperty(final String name) {
        
        return (String) AccessController
                .doPrivileged(new java.security.PrivilegedAction() {
                    
                    public Object run() {
                        return System.getProperty(name);
                        
                    }
                    
                });
    }
    
    /**
     * Check whether this is sun jvm.
     *
     * @return true if it is sun jvm, false if it is not sun jvm
     */
    public static final boolean isSunJVM() {
        String vendor = getSystemProperty("java.vendor");
        return "Sun Microsystems Inc.".equals(vendor)
                || "Oracle Corporation".equals(vendor);
    }
    
    /**
     * Check whether this is IBM jvm.
     *
     * @return true if it is IBM jvm, false if it is not IBM jvm
     */
    public static final boolean isIBMJVM() {
        return ("IBM Corporation".equals(getSystemProperty("java.vendor")));
    }
    
    /**
     * For IBM jvm, this method will dump more diagnostic information to file.
     * JVM specific code for other vender can be added. DERBY-4856 
     *  
     */
    public static void javaDump() {
        if (isIBMJVM()) {
            Class ibmc = null;
            try {
                ibmc = Class.forName("com.ibm.jvm.Dump");
                final Method ibmm = ibmc.getMethod("JavaDump", new Class[] {});
                
                AccessController.doPrivileged(new PrivilegedExceptionAction() {
                    public Object run() throws IllegalAccessException,
                            MalformedURLException, InstantiationException,
                            InvocationTargetException {
                        return ibmm.invoke(null, new Object[] {});
                    }
                });
            } catch (Exception e) {
                if (SanityManager.DEBUG) {
                    SanityManager
                            .THROWASSERT(
                                    "Failed to execute com.ibm.jvm.Dump.JavaDump in IBM JVM",
                                    e);
                }
            }
        }
    }
}
