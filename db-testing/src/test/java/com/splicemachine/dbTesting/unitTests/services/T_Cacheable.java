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

import com.splicemachine.db.iapi.services.cache.*;

import com.splicemachine.db.iapi.error.StandardException;

/**

*/
public abstract class T_Cacheable implements Cacheable {

	protected boolean	isDirty;

	protected Thread       owner;
		
	public T_Cacheable() {
	}

	/*
	** Cacheable methods
	*/

	public Cacheable setIdentity(Object key) throws StandardException {
		// we expect a key of type Object[]
		if (!(key instanceof T_Key)) {
			throw T_CacheException.invalidKey();
		}

		owner = null;

		return null; // must be overriden by super class	
	}



	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {

		// we expect a key of type Object[]
		if (!(key instanceof T_Key)) {
			throw T_CacheException.invalidKey();
		}

		owner = (Thread) createParameter;

		return null; // must be overriden by super class
	}

	/**
		Returns true of the object is dirty. Will only be called when the object is unkept.

		<BR> MT - thread safe 

	*/
	public boolean isDirty() {
		synchronized (this) {
			return isDirty;
		}
	}



	public void clean(boolean forRemove) throws StandardException {
		synchronized (this) {
			isDirty = false;
		}
	}

	/*
	** Implementation specific methods
	*/

	protected Cacheable getCorrectObject(Object keyValue) throws StandardException {

		Cacheable correctType;

		if (keyValue instanceof Integer) {

			correctType = new T_CachedInteger();
		//} else if (keyValue instanceof String) {
			//correctType = new T_CachedString();
		} else {

			throw T_CacheException.invalidKey();
		}

		return correctType;
	}

	protected boolean dummySet(T_Key tkey) throws StandardException {

		// first wait
		if (tkey.getWait() != 0) {
			synchronized (this) {

				try {
					wait(tkey.getWait());
				} catch (InterruptedException ie) {
					// RESOLVE
				}
			}
		}

		if (!tkey.canFind())
			return false;

		if (tkey.raiseException())
			throw T_CacheException.identityFail();

		return true;
	}

	public void setDirty() {
		synchronized (this) {
			isDirty = true;
		}
	}

	public boolean canRemove() {

		synchronized (this) {
			if (owner == null)
				owner = Thread.currentThread();

			if (owner == Thread.currentThread())
				return true;
			return false;
		}
	}
}

