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

package com.splicemachine.db.impl.services.bytecode;

import java.lang.reflect.*;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import java.util.Hashtable;

/**
 * Validate BC calls.
 *
 */
class d_BCValidate
{

	private static final String[] csPackages = {
		"java",
		"com.splicemachine.db.exe.",
		"com.splicemachine.db.iapi.",
		"com.splicemachine.db.jdbc.",
		"com.splicemachine.db.iapi.",
		"com.splicemachine.db.impl.",
		"com.splicemachine.db.authentication.",
		"com.splicemachine.db.catalog.",
		"com.splicemachine.db.iapi.db.",
		"com.splicemachine.db.iapi.types.",
		"com.splicemachine.db.iapi.types.",
		"com.splicemachine.db.catalog.types.",
		};


	private static final Class[] NO_PARAMS = new Class[0];

	static void checkMethod(short opcode, Type dt, String methodName, String[] debugParameterTypes, Type rt) {


		if (SanityManager.DEBUG) {
			String reason = null;
			try {

				String declaringClass = dt.javaName();
				if (declaringClass.startsWith("com.splicemachine.db.exe."))
					return;

				// only validate against Derby engine or Java classes. Not user defined classes
				int p;
				for (p = 0; p < csPackages.length; p++) {
					if (declaringClass.startsWith(csPackages[p]))
						break;
				}
				if (p == csPackages.length)
					return;

				Class[] params = NO_PARAMS;

				Class declaring = loadClass(declaringClass);

				if (debugParameterTypes != null) {
					params = new Class[debugParameterTypes.length];
					for (int i = 0; i < debugParameterTypes.length; i++) {
						params[i] = loadClass(debugParameterTypes[i]);
					}

				}
				
				// If the class is not in the same class loader then it
				// it must be a non-Derby class. In that case any method etc.
				// being accessed must be public, so don't use the getDeclared
				// methods. Default SecurityManager behaviour is to grant access to public members
				// and members from classes loaded by the same class loader. Thus
				// we try to fall into these categories to avoid having to grant
				// permissions to db jars for the function tests.
				
				ClassLoader declareLoader = declaring.getClassLoader();
				ClassLoader myLoader = d_BCValidate.class.getClassLoader();
				
				boolean sameClassLoader = false;
				if (declareLoader == myLoader)
					sameClassLoader = true;
				else if (declareLoader != null)
					sameClassLoader = declareLoader.equals(myLoader);
				
				String actualReturnType;

				if (methodName.equals("<init>")) {
					Constructor c;
					
					if (sameClassLoader)
					{
						c = declaring.getDeclaredConstructor(params);
					}
					else
					{
						c = declaring.getConstructor(params);
						
						// check this construct is declared by this
						// class, has to be, right? But no harm checking.
						if (!c.getDeclaringClass().equals(declaring))
						{
							reason = "constructor " + c.toString() + " declared on " + c.getDeclaringClass() + " expected " + declaring;
						}
					}
					
					actualReturnType = "void";
				} else {
					Method m;
					
					if (sameClassLoader)
					{
						m = declaring.getDeclaredMethod(methodName, params);
					}
					else
					{
						m = declaring.getMethod(methodName, params);
						
						// check this method is declared by this
						// class? But no harm checking.
						if (!m.getDeclaringClass().equals(declaring))
						{
							reason = "method " + m.toString() + " declared on " + m.getDeclaringClass() + " expected " + declaring;
						}
					}
					
					actualReturnType = m.getReturnType().getName();
				}
				
				// do we already have a problem?
				if (reason == null)
				{

					Class requestedReturnType = loadClass(rt.javaName());
	
					// check the return type
					if (actualReturnType.equals(requestedReturnType.getName())) {
	
						// check the inteface match
						if (opcode != VMOpcode.INVOKEINTERFACE)
							return;
	
						if (declaring.isInterface())
							return;
	
						reason = "declaring class is not an interface";
	
					} else {
						reason = "return type is " + actualReturnType;
					}
				}


			} catch (Exception e) {
				reason = e.toString();
				e.printStackTrace(System.out);
			}

			StringBuilder sig = new StringBuilder(dt.javaName() + " >> " + rt.javaName() + " " + methodName + "(");
			if (debugParameterTypes != null) {
				for (int i = 0; i < debugParameterTypes.length; i++) {
					if (i != 0)
						sig.append(", ");
					sig.append(debugParameterTypes[i]);
				}
			}
			sig.append(")");

			String msg = "Invalid method " + sig + " because " + reason;

			System.out.println(msg);
			SanityManager.THROWASSERT(msg);
		}
	}

	private static Hashtable primitives;

	static {
		if (SanityManager.DEBUG) {
			primitives = new Hashtable();
			primitives.put("boolean", Boolean.TYPE);
			primitives.put("byte", Byte.TYPE);
			primitives.put("char", Character.TYPE);
			primitives.put("double", Double.TYPE);
			primitives.put("float", Float.TYPE);
			primitives.put("int", Integer.TYPE);
			primitives.put("long", Long.TYPE);
			primitives.put("short", Short.TYPE);
			primitives.put("void", Void.TYPE);
		}

	}
	

	private static Class loadClass(String name) throws ClassNotFoundException {

		if (SanityManager.DEBUG) {

			Class c = (Class) primitives.get(name);
			if (c != null)
				return c;

			if (name.endsWith("[]")) {
				Class baseClass = loadClass(name.substring(0, name.length() - 2));
				return Array.newInstance(baseClass, 0).getClass();
			}
			
			return Class.forName(name);
		}

		return null;
	}
}
