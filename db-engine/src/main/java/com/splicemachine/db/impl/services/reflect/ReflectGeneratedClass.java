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

import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.loader.GeneratedByteCode;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.context.Context;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public final class ReflectGeneratedClass extends LoadedGeneratedClass {

	private final ConcurrentHashMap<String, GeneratedMethod> methodCache;
	private static final GeneratedMethod[] directs;


	private final Class	factoryClass;
	private GCInstanceFactory factory;

	static {
		directs = new GeneratedMethod[10];
		for (int i = 0; i < directs.length; i++) {
			directs[i] = new DirectCall(i);
		}
	}

	public ReflectGeneratedClass(ClassFactory cf, Class jvmClass, Class factoryClass) {
		super(cf, jvmClass);
		methodCache = new ConcurrentHashMap<>();
		this.factoryClass = factoryClass;
	}

	public Object newInstance(Context context) throws StandardException	{
		if (factoryClass == null) {
			return super.newInstance(context);
		}

		if (factory == null) {

			Throwable t;
			try {
				factory =  (GCInstanceFactory) factoryClass.newInstance();
				t = null;
			} catch (InstantiationException | LinkageError | IllegalAccessException ie) {
				t = ie;
			}

            if (t != null)
				throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
		}

		GeneratedByteCode ni = factory.getNewInstance();
		ni.initFromContext(context);
		ni.setGC(this);
		ni.postConstructor();
		return ni;

	}

	public GeneratedMethod getMethod(String simpleName)
		throws StandardException {

		GeneratedMethod rm = methodCache.get(simpleName);
		if (rm != null)
			return rm;

		// Only look for methods that take no arguments
		try {
			if ((simpleName.length() == 2) && simpleName.startsWith("e")) {

				int id = ((int) simpleName.charAt(1)) - '0';

				rm = directs[id];


			}
			else
			{
				Method m = getJVMClass().getMethod(simpleName, (Class []) null);
				
				rm = new ReflectMethod(m);
			}
			methodCache.put(simpleName, rm);
			return rm;

		} catch (NoSuchMethodException nsme) {
			throw StandardException.newException(SQLState.GENERATED_CLASS_NO_SUCH_METHOD,
				nsme, getName(), simpleName);
		}
	}
}

class DirectCall implements GeneratedMethod {

	private final int which;

	DirectCall(int which) {

		this.which = which;
	}

	public Object invoke(Object ref)
		throws StandardException {

		try {

			GeneratedByteCode gref = (GeneratedByteCode) ref;
			switch (which) {
			case 0:
				return gref.e0();
			case 1:
				return gref.e1();
			case 2:
				return gref.e2();
			case 3:
				return gref.e3();
			case 4:
				return gref.e4();
			case 5:
				return gref.e5();
			case 6:
				return gref.e6();
			case 7:
				return gref.e7();
			case 8:
				return gref.e8();
			case 9:
				return gref.e9();
			}
			return null;
		} catch (StandardException se) {
			throw se;
		}		
		catch (Throwable t) {
			throw StandardException.unexpectedUserException(t);
		}
	}

//	@Override
	public String getMethodName() {
		return "e"+which;
	}
}
