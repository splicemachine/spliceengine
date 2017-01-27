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

package com.splicemachine.dbTesting.functionTests.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;


/**
 * Code to aid in checking the Security of Derby.
 * This initial implementation only handles the emebdded code.
 * Future work could expand to the client driver and network server.
 */
public class SecurityCheck {
	
	/**
	 * List of classes in the public api for the embedded engine.
	 */
	private static final String[] EMBEDDED_PUBLIC_API =
	{
		"com.splicemachine.db.jdbc.EmbeddedDriver",
		"com.splicemachine.db.jdbc.EmbeddedDataSource",
		"com.splicemachine.db.jdbc.EmbeddedSimpleDataSource",
		"com.splicemachine.db.jdbc.EmbeddedConnectionPoolDataSource",
		"com.splicemachine.db.jdbc.EmbeddedXADataSource",
		"com.splicemachine.db.authentication.UserAuthenticator",
	};
	
	/**
	 * Is the passed in class part of the declared public api.
	 * Currently only the emebdded public api
	 * @param c class to be checked
	 * @return true if the class is part of the public api, false otherwise.
	 */
	private static boolean isPublicApi(Class c)
	{
		for (int i = 0; i < EMBEDDED_PUBLIC_API.length; i++)
		{
			if (EMBEDDED_PUBLIC_API[i].equals(c.getName()))
				return true;
		}
		return false;
	}
	
	/**
	 * Map of all classes that have been inspected.
	 * The key is the class name, if the value is null then
	 * the class is being inspected. Otherwise the value is
	 * a String description of the risks.
	 */
	private static final SortedMap allInspectedClasses = new TreeMap();
	
	/**
	 * Perform security analysis of the public api for the embedded engine.
	 * Prints a report to System.out on completion.
	 * @throws ClassNotFoundException
	 */
	public static void checkEmbeddedPublicApi() throws ClassNotFoundException
	{
		System.out.println("SecurityCheck: embedded public api classes");
		allInspectedClasses.clear();
		for (int i = 0; i < EMBEDDED_PUBLIC_API.length; i++)
			SecurityCheck.inspectClass(EMBEDDED_PUBLIC_API[i]);

		SecurityCheck.report(true);
	}
	
	/**
	 * Produce a report on System.out of all inspected classes
	 * that have risks associated with them.
	 *
	 */
	public static void report()
	{
        SecurityCheck.report(false);
	}
	
	/**
	 * Produce a report on System.out of all inspected classes
	 * that have risks associated with them. If reportClear is
	 * true then additionally all classes that have been inspected
	 * will be returned.
	 *
	 */
	private static void report(boolean reportClear)
	{
		synchronized (allInspectedClasses)
		{
		for (Iterator it = allInspectedClasses.keySet().iterator(); it.hasNext(); )
		{
			Object key = it.next();
			Object value = allInspectedClasses.get(key);
			if (value == null)
			{
				if (reportClear)
				    System.out.println("CLEAR: " + key);
			}
			else
			{
				System.out.print(value);
			}
		}
		}
	}
	
	/**
	 * Inspect a class for security risks. No output is generated
	 * by this call, the caller must call report() to obtain the risks.
	 * @param className
	 * @throws ClassNotFoundException
	 */
	public static void inspectClass(String className) throws ClassNotFoundException
	{			
		SecurityCheck.inspectClass(Class.forName(className), null);
	}	
	
	/**
	 * Inspect the class of the passed in Object for security risks.
	 * This inspects, at this level only, the actual type of
	 * the object, not the declared type. E.g. for DriverManager.getConnection
	 * the declared type is java.sql.Connection which has no security risks,
	 * but the implementation type returned may have many.
	 * 
	 * <code>
	 * Connection conn = DriverManager.getConnection(url);
	 * // will inspect the implementation call, eg. EmbedConnection30
	 * SecurityManager.inspect(conn);
	 * </code>
	 * No output is generated by this call,
	 * the caller must call report() to obtain the risks.
	 * @param o Obejct to be inspected
	 * @param declared the declared type of the object. 
	 */
    public static void assertSourceSecurity(Object o, String declared)
    {
        Assert.assertNotNull(o);
        Assert.assertTrue(SecurityCheck.inspectClass(o.getClass(), declared));
    }
    
	/**
	 * Inspect a Derby class for security risks. This includes following potential
	 * references exposed through the class.
	 * <P>
	 * Risks looked at:
	 * <UL>
	 * <LI> public constructors in non-public class - 
	 * No justification for the constructor to be public.
	 * <LI> public constructors in non-final class and non-sealed package -
	 * Allows the class to be sub-classes through a injected class in
	 * the same package.
	 * <LI> public non-final field - Allows any one with a handle to
	 * the object to change the field.
     * </UL>
	 * <P>
	 * The type of any public field or return type of any public method
	 * is also inspected. The assumption is that if such a field or method
	 * exists they have the potential to be called and return a valid object.
	 * <P>
	 * Note that this inspection is through the declared type of exposed
	 * references, not the actual runtime type. The actual runtime type
	 * might expose other classes that should be inspected.
	 * @param c the class to inspect
	 * @throws ClassNotFoundException
	 */
	private static boolean inspectClass(Class c, String declared)
	{		
		if (!c.getName().startsWith("com.splicemachine.db."))
			return false;
		
		// Initial focus on embedded engine
		if (c.getName().startsWith("com.splicemachine.db.client."))
			return false;
		
		synchronized (allInspectedClasses)
		{
		if (allInspectedClasses.containsKey(c.getName()))
			return true;
		
		allInspectedClasses.put(c.getName(), null);
				
		StringBuffer sb = new StringBuffer();
		
		sb.append("Class ");
		sb.append(c.getName());
		sb.append('\n');
		
		if (declared != null)
		{
			allInspectedClasses.put(declared, "Checked class declared as: " + declared + "\n");
			
		}

		boolean isPublicApi = SecurityCheck.isPublicApi(c);
	
		boolean hasIssues = false;
		
		boolean isSealed = c.getPackage().isSealed();
		boolean isFinal = Modifier.isFinal(c.getModifiers());
		boolean isPublic = Modifier.isPublic(c.getModifiers());
		boolean isAbstract = Modifier.isAbstract(c.getModifiers());
		
		Constructor[] constructors = c.getConstructors();
		
		boolean hasPublicConstructor = constructors.length != 0;
		
		if (hasPublicConstructor && !isPublic)
		{
			hasIssues = true;
			
			// No reason for a public constructor in a non-public class
			sb.append("..public constructors in non-public class\n");
			
			// class can be sub-classed even though it is not public
			if (!isFinal && !isSealed)
				sb.append("..public constructors in non-final class and non-sealed package\n");
		}
		
		if (hasPublicConstructor && isPublic)
		{
			// TODO: Need to work on these checks.
			if (!isPublicApi)
			{
			    //hasIssues = true;
			
			    // anyone can create instances of this class
			    //sb.append("..public constructors in public class\n");
			}

			// and anyone can sub-class this class
			if (!isFinal)
			{
				//hasIssues = true;
				//sb.append("..public constructors in public non-final class\n");
			}
		}
		
		for (int i = 0; i < constructors.length; i++)
		{
			if (hasIssues)
			{
				sb.append("..public constructor: ");
				sb.append(constructors[i].toString());
				sb.append('\n');
			}
		}
		
		Field[] fields = c.getFields();
		for (int i = 0; i < fields.length; i++)
		{
			Field f = fields[i];
			boolean isStatic = Modifier.isStatic(f.getModifiers());
						
			Class fieldType = f.getType();
			SecurityCheck.inspectClass(fieldType, null);
			
			if (Modifier.isFinal(f.getModifiers()))
			{
				// TODO: Should this be a concern if non-static?
				continue;
			}
			
			hasIssues = true;
			sb.append("..public non-final field: ");
			sb.append(f.toString());
			sb.append('\n');
		}

		Method[] methods = c.getMethods();
		for (int i = 0; i < methods.length; i++)
		{
			Method m = methods[i];
						
			Class methodType = m.getReturnType();
			if (SecurityCheck.inspectClass(methodType, null))
			{
				// method returns a class of interest to us.
				
				// just a method returning a public api
				if (SecurityCheck.isPublicApi(methodType))
					continue;
				
				/*
				 * Not sure this is a vaild risk.
				hasIssues = true;
				sb.append("..public method returning non-public api class: ");
				sb.append(m.toString());
				sb.append("\n");
				*/
			}
			
		}		
		if (hasIssues)
			allInspectedClasses.put(c.getName(), sb.toString());
		}
		
		return true;
		
	}
}
