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

package com.splicemachine.db.impl.services.locks;

import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.locks.Lockable;
import com.splicemachine.db.iapi.services.locks.C_LockFactory;

import com.splicemachine.db.iapi.error.StandardException;

/**
	A Lock represents a granted or waiting lock request.

	<BR>
	MT - Mutable - Immutable identity : Thread Aware
*/

public final class ActiveLock extends Lock {

	/**
		Set to true if the object waiting on this thread should wake up,
		MT - mutable - java synchronized(this)
	*/
	byte wakeUpNow;

	/**
		Set to true if the Lock potentially could be granted.

		MT - mutable - single thread required
	*/
	boolean potentiallyGranted;

	/**
		If true then this lock can be granted even if
		it is not the first lock request on the wait queue.
		This can occur if the compatibility space already holds
		a lock on the object.
	*/
	protected boolean canSkip;

	/**
		Initialize the lock, should be seen as part of the constructor. A future
		version of this class may become mutable - mutable identity.

		MT - single thread required
	*/
	protected ActiveLock(CompatibilitySpace space, Lockable ref,
						 Object qualifier) {
		super(space, ref, qualifier);
	}

	/**	
		Set the potentially granted flag, returns true if the
		flag changed its state.

		MT - single thread required
	*/
	protected boolean setPotentiallyGranted() {
		if (!potentiallyGranted) {
			potentiallyGranted = true;
			return true;
		}
		return false;
	}

	/**	
		Clear the potentially granted flag.

		MT - single thread required
	*/
	protected void clearPotentiallyGranted() {
		potentiallyGranted = false;
	}

	/**
		Wait for a lock to be granted, returns when the lock is granted.
		<P>
		The sleep wakeup scheme depends on the two booleans wakeUpNow & potentiallyGranted.
		  
		MT - Single thread required - and assumed to be the thread requesting the lock.

		@return true if the wait ended early (ie. someone else woke us up).

		@exception StandardException timeout, deadlock or thread interrupted
	*/
	protected synchronized byte waitForGrant(int timeout)
		throws StandardException
	{

		if (wakeUpNow == Constants.WAITING_LOCK_IN_WAIT) {

			try {


				if (timeout == C_LockFactory.WAIT_FOREVER) {
					wait();
				}
				else if (timeout > 0) {
					wait(timeout);
				}

			} catch (InterruptedException ie) {
                wakeUpNow = Constants.WAITING_LOCK_INTERRUPTED;
			}
		}

		byte why = wakeUpNow;
		wakeUpNow = Constants.WAITING_LOCK_IN_WAIT;
		return why;
	}

	/**
		Wake up anyone sleeping on this lock.

		MT - Thread Safe
	*/
	protected synchronized void wakeUp(byte why) {
		// If we were picked as a deadlock victim then don't
		// override the wakeup reason with another one.
		if (wakeUpNow != Constants.WAITING_LOCK_DEADLOCK)
			wakeUpNow = why;
		notify();
	}
}

