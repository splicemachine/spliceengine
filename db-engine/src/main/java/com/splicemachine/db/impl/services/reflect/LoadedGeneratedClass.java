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

import com.splicemachine.db.iapi.services.loader.GeneratedByteCode;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.services.context.Context;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.loader.ClassInfo;


public abstract class LoadedGeneratedClass
	implements GeneratedClass
{

	/*
	** Fields
	*/

	private final ClassInfo	ci;
	private final int classLoaderVersion;

	/*
	**	Constructor
	*/

	public LoadedGeneratedClass(ClassFactory cf, Class jvmClass) {
		ci = new ClassInfo(jvmClass);
		classLoaderVersion = cf.getClassLoaderVersion();
	}

	/*
	** Public methods from Generated Class
	*/

	public String getName() {
		return ci.getClassName();
	}

	public Object newInstance(Context context) throws StandardException	{

		Throwable t;
		try {
			GeneratedByteCode ni =  (GeneratedByteCode) ci.getNewInstance();
			ni.initFromContext(context);
			ni.setGC(this);
			ni.postConstructor();
			return ni;

		} catch (InstantiationException | LinkageError | java.lang.reflect.InvocationTargetException | IllegalAccessException ie) {
			t = ie;
		}

        throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
	}

	public final int getClassLoaderVersion() {
		return classLoaderVersion;
	}

	/*
	** Methods for subclass
	*/
	protected Class getJVMClass() {
		return ci.getClassObject();
	}
}
