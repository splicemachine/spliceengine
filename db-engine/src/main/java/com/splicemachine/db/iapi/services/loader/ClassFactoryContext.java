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

package com.splicemachine.db.iapi.services.loader;

import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.property.PersistentSet;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * Context that provides the correct ClassFactory for the
 * current service. Allows stateless code to obtain the
 * correct class loading scheme.
*/
public abstract class ClassFactoryContext extends ContextImpl {

	public static final String CONTEXT_ID = "ClassFactoryContext";

	private final ClassFactory cf;

	protected ClassFactoryContext(ContextManager cm, ClassFactory cf) {

		super(cm, CONTEXT_ID);

		this.cf = cf;
	}

	public final ClassFactory getClassFactory() {
		return cf;
	}

    /**
     * Get the lock compatibility space to use for the
     * transactional nature of the class loading lock.
     * Used when the classpath changes or a database
     * jar file is installed, removed or replaced.
     */
    public abstract CompatibilitySpace getLockSpace() throws StandardException;

    /**
     * Get the set of properties stored with this service.
    */
	public abstract PersistentSet getPersistentSet() throws StandardException;

	/**
		Get the mechanism to rad jar files. The ClassFactory
		may keep the JarReader reference from the first class load.
	*/
	public abstract JarReader getJarReader();

    /**
     * Handle any errors. Only work here is to pop myself
     * on a session or greater severity error.
     */
	public final void cleanupOnError(Throwable error) {
        if (error instanceof StandardException) {

            StandardException se = (StandardException) error;
            
            if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
                popMe();
        }
    }
}
