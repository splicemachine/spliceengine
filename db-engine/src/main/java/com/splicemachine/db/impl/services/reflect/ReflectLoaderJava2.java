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

package com.splicemachine.db.impl.services.reflect;

import com.splicemachine.db.iapi.util.ByteArray;

public final class ReflectLoaderJava2 extends ClassLoader {

	/*
	**	Fields
	*/

	private final DatabaseClasses cf;
	
	/*
	** Constructor
	*/

	public ReflectLoaderJava2(ClassLoader parent, DatabaseClasses cf) {
		super(parent);
		this.cf = cf;
	}

	protected Class findClass(String name)
		throws ClassNotFoundException {
		return cf.loadApplicationClass(name);
	}

	/*
	** Implementation specific methods
	** NOTE these are COPIED from ReflectLoader as the two classes cannot be made into
	   a super/sub class pair. Because the Java2 one needs to call super(ClassLoader)
	   that was added in Java2 and it needs to not implement loadClass()
	*/

	/**
		Load a generated class from the passed in class data.
	*/
	public LoadedGeneratedClass loadGeneratedClass(String name, ByteArray classData) {

		Class jvmClass = defineClass(name, classData.getArray(), classData.getOffset(), classData.getLength());

		resolveClass(jvmClass);

		/*
			DJD - not enabling this yet, need more memory testing, may only
			create a factory instance when a number of instances are created.
			This would avoid a factory instance for DDL

		// now generate a factory class that loads instances
		int lastDot = name.lastIndexOf('.');
		String factoryName = name.substring(lastDot + 1, name.length()).concat("_F");

		classData = cf.buildSpecificFactory(name, factoryName);
		Class factoryClass = defineClass(CodeGeneration.GENERATED_PACKAGE_PREFIX.concat(factoryName),
			classData.getArray(), classData.getOffset(), classData.getLength());
		resolveClass(factoryClass);
		
		  */
		Class factoryClass = null;

		return new ReflectGeneratedClass(cf, jvmClass, factoryClass);
	}
}
