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

package com.splicemachine.db.iapi.services.loader;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.util.ByteArray;

import java.io.ObjectStreamClass;


/**
	A class factory module to handle application classes
	and generated classes.
*/

public interface ClassFactory {

	/**
		Add a generated class to the class manager's class repository.

		@exception 	StandardException	Standard Derby error policy

	*/
	GeneratedClass loadGeneratedClass(String fullyQualifiedName, ByteArray classDump)
		throws StandardException;

	/**
		Return a ClassInspector object
	*/
	ClassInspector	getClassInspector();

	/**
		Load an application class, or a class that is potentially an application class.

		@exception ClassNotFoundException Class cannot be found, or
		a SecurityException or LinkageException was thrown loading the class.
	*/
	Class loadApplicationClass(String className)
		throws ClassNotFoundException;

	/**
		Load an application class, or a class that is potentially an application class.

		@exception ClassNotFoundException Class cannot be found, or
		a SecurityException or LinkageException was thrown loading the class.
	*/
	Class loadApplicationClass(ObjectStreamClass classDescriptor)
		throws ClassNotFoundException;

	/**
		Was the passed in class loaded by a ClassManager.

		@return true if the class was loaded by a Derby class manager,
		false it is was loaded by the system class loader, or another class loader.
	*/
	boolean isApplicationClass(Class theClass);

	/**
		Notify the class manager that a jar file has been modified.
		@param reload Restart any attached class loader

		@exception StandardException thrown on error
	*/
	void notifyModifyJar(boolean reload) throws StandardException ;

	/**
		Notify the class manager that the classpath has been modified.

		@exception StandardException thrown on error
	*/
	void notifyModifyClasspath(String classpath) throws StandardException ;

	/**
		Return the in-memory "version" of the class manager. The version
		is bumped everytime the classes are re-loaded.
	*/
	int getClassLoaderVersion();
}
