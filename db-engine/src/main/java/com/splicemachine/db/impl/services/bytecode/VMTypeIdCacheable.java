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

package com.splicemachine.db.impl.services.bytecode;

import com.splicemachine.db.iapi.services.cache.Cacheable;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.classfile.ClassHolder;

/**
 * This class implements a Cacheable for a Byte code generator cache of
 * VMTypeIds.  It maps a Java class or type name to a VM type ID.
 */
class VMTypeIdCacheable implements Cacheable {
	/* The VM name of the Java class name */
	// either a Type (java type) or a String (method descriptor)
	private Object descriptor;

	/* This is the identity */
	private Object key;

	/* Cacheable interface */

	/** @see Cacheable#clearIdentity */
	public void clearIdentity() {
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity() {
		return key;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("VMTypeIdCacheable.create() called!");
		}
		return this;
	}

	/** @see Cacheable#setIdentity */
	public Cacheable setIdentity(Object key) {

		this.key = key;
		if (key instanceof String) {
			/* The identity is the Java class name */
			String javaName = (String) key;

			/* Get the VM type name associated with the Java class name */
			String vmName = ClassHolder.convertToInternalDescriptor(javaName);
			descriptor = new Type(javaName, vmName);
		}
		else
		{
			descriptor = ((BCMethodDescriptor) key).buildMethodDescriptor();
		}

		return this;
	}

	/** @see Cacheable#clean */
	public void clean(boolean remove) {
		/* No such thing as a dirty cache entry */
		return;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty() {
		/* No such thing as a dirty cache entry */
		return false;
	}

	/*
	** Class specific methods.
	*/

	/**
	 * Get the VM Type name (java/lang/Object) that is associated with this Cacheable
	 */

	Object descriptor() {
		return descriptor;
	}
}
