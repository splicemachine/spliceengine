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

package com.splicemachine.db.impl.services.reflect;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import com.splicemachine.db.iapi.util.ByteArray;

/**
	Reflect loader with Privileged block for Java 2 security. 
*/

public class ReflectClassesJava2 extends DatabaseClasses implements java.security.PrivilegedAction {
	protected ByteArrayClassMap preCompiled;
	private int action = -1;

	protected LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump) {
		if (preCompiled == null)
			preCompiled = new ByteArrayClassMap(100);
		if (classDump == null || classDump.getArray() == null) {
			// not a generated class, just load the class directly.
			try {
				Class jvmClass = Class.forName(fullyQualifiedName);
				return new ReflectGeneratedClass(this, jvmClass, null);
			} catch (ClassNotFoundException cnfe) {
				throw new NoClassDefFoundError(cnfe.toString());
			}
		} else {
			LoadedGeneratedClass classAttempt = (LoadedGeneratedClass) preCompiled.get(fullyQualifiedName);
			if (classAttempt != null)
				return classAttempt;			
			action = 1;
			classAttempt = ((ReflectLoaderJava2) java.security.AccessController.doPrivileged(this)).loadGeneratedClass(fullyQualifiedName, classDump);
			preCompiled.put(fullyQualifiedName, classAttempt);
			return classAttempt;
		}
	}

	public final Object run() {

		try {
			// SECURITY PERMISSION - MP2
			switch (action) {
			case 1:
				return new ReflectLoaderJava2(getClass().getClassLoader(), this);
			case 2:
				return Thread.currentThread().getContextClassLoader();
			default:
				return null;
			}
		} finally {
			action = -1;
		}
		
	}

	protected Class loadClassNotInDatabaseJar(String name) throws ClassNotFoundException {
		
		Class foundClass = null;
		
	    // We may have two problems with calling  getContextClassLoader()
	    // when trying to find our own classes for aggregates.
	    // 1) If using the URLClassLoader a ClassNotFoundException may be 
	    //    thrown (Beetle 5002).
	    // 2) If Derby is loaded with JNI, getContextClassLoader()
	    //    may return null. (Beetle 5171)
	    //
	    // If this happens we need to user the class loader of this object
	    // (the classLoader that loaded Derby). 
	    // So we call Class.forName to ensure that we find the class.
        try {
        	ClassLoader cl;
        	synchronized(this) {
        	  action = 2;
              cl = ((ClassLoader)
			      java.security.AccessController.doPrivileged(this));
        	}
			
			foundClass = (cl != null) ?  cl.loadClass(name) 
				      :Class.forName(name);
        } catch (ClassNotFoundException cnfe) {
            foundClass = Class.forName(name);
        }
		return foundClass;
	}
	
	public static class ByteArrayClassMap extends LinkedHashMap {
		private int maxCapacity;
		public ByteArrayClassMap(int maxCapacity){
			super(0, 0.75F,true); // LRU CACHE
			this.maxCapacity = maxCapacity;
		}
		protected boolean removeEldestEntry(Entry eldest) {
			return size() >= this.maxCapacity;
		}
	}
}

