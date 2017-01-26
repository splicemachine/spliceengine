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

import com.splicemachine.db.iapi.services.locks.*;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import java.util.Hashtable;

/**
	Unit test Lockable
	<BR>
	A simple Lockable that allows a single locker, but that locker
	can lock the object multiple times, standard Lockable behaviour.
*/

class T_L1 implements Lockable {

	long value = 0;
	int count = 0;

	Latch latch;

	T_L1() {
	}

	/*
	** Lockable methods (Simple, qualifier assumed to be null).
	*/

	/** 
		Qualififier is assumed to be null.
	@see Lockable#lockEvent
	*/
	public void lockEvent(Latch lockInfo) {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(lockInfo.getQualifier() == null);

		latch = lockInfo;

		count++;
	}

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		return false;
	}

	public boolean lockerAlwaysCompatible() {
		return true;
	}

	/** 
		Qualififier is assumed to be null.
	@see Lockable#unlockEvent
	*/
	public void unlockEvent(Latch lockInfo) {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(lockInfo.getQualifier() == null);
		
		count--;
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(count >= 0);
		latch = null;
	}

	public boolean lockAttributes(int flag, Hashtable t)
	{
		return false;
	}
}
