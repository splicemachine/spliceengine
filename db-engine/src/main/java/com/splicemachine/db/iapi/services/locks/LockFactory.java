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

package com.splicemachine.db.iapi.services.locks;

import com.splicemachine.db.iapi.util.Matchable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import java.util.Enumeration;


/**
    Generic locking of objects. Enables deadlock detection.

  <BR>
    MT - Mutable - Container Object - Thread Safe


*/
public interface LockFactory extends PropertySetCallback {

	/**
	 * Create an object which can be used as a compatibility space. A
	 * compatibility space object can only be used in the
	 * <code>LockFactory</code> that created it.
	 *
	 * @param owner the owner of the compatibility space (typically a
	 * transaction object). Might be <code>null</code>.
	 * @return an object which represents a compatibility space
	 */
	CompatibilitySpace createCompatibilitySpace(LockOwner owner);

	/**
		Lock an object within a compatibility space
		and associate the lock with a group object,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatible and granted by the lock manager if the trio
        {compatibilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier and compatibilitySpace, equals() method for ref).
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatibility space is not assumed to be owned by a single thread.
	


		@param compatibilitySpace object defining compatibility space
		@param group handle of group, must be private to a thread.
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

		@exception com.splicemachine.db.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception com.splicemachine.db.iapi.error.StandardException The wait for the lock timed out (message id will be LockFactory.TimeOut).
		@exception com.splicemachine.db.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Derby error policy.

	*/
	boolean lockObject(CompatibilitySpace compatibilitySpace,
					   Object group, Lockable ref, Object qualifier,
					   int timeout)
		throws StandardException;

	/**
		Unlock a single lock on a single object held within this compatibility
		space and locked with the supplied qualifier.

		@param compatibilitySpace object defining compatibility space
		@param group handle of group.
		@param ref Reference to object to be unlocked.
		@param qualifier qualifier of lock to be unlocked

		@return number of locks released (one or zero).
	*/
	int unlock(CompatibilitySpace compatibilitySpace, Object group,
			   Lockable ref, Object qualifier);

	/**
		Unlock all locks in a group. 

		@param compatibilitySpace object defining compatibility space
		@param group handle of group that objects were locked with.
	*/
	void unlockGroup(CompatibilitySpace compatibilitySpace,
					 Object group);

	/**
		Unlock all locks on a group that match the passed in value.
	*/
	void unlockGroup(CompatibilitySpace compatibilitySpace,
					 Object group, Matchable key);

	/**
		Transfer a set of locks from one group to another.
	*/
	void transfer(CompatibilitySpace compatibilitySpace,
				  Object oldGroup, Object newGroup);

	/**
		Returns true if locks held by anyone are blocking anyone else
	*/
	boolean anyoneBlocked();

	/**
		Return true if locks are held in this compatibility space and
		 this group.

		@param group handle of group that objects were locked with.

	*/
	boolean areLocksHeld(CompatibilitySpace compatibilitySpace,
						 Object group);

	/**
		Return true if locks are held in this compatibility space.
	*/
	boolean areLocksHeld(CompatibilitySpace compatibilitySpace);

	/**
		Lock an object with zero duration within a compatibility space,
		waits up to timeout milli-seconds for the object to become unlocked. A 
        timeout of 0 means do not wait for the lock to be unlocked.
		Note the actual time waited is approximate.
		<P>
		Zero duration means the lock is released as soon as it is obtained.
		<P>
		A compatibility space in an space where lock requests are assumed to be
        compatible and granted by the lock manager if the trio
        {compatibilitySpace, ref, qualifier} are equal (i.e. reference equality
        for qualifier and compatibilitySpace, equals() method for ref).
		Granted by the lock manager means that the Lockable object may or may 
        not be queried to see if the request is compatible.
		<BR>
		A compatibility space is not assumed to be owned by a single thread.
	


		@param compatibilitySpace object defining compatibility space
		@param ref reference to object to be locked
		@param qualifier A qualification of the request.
		@param timeout the maximum time to wait in milliseconds, LockFactory.NO_WAIT means don't wait.

		@return true if the lock was obtained, false if timeout is equal to LockFactory.NO_WAIT and the lock
		could not be granted.

		@exception com.splicemachine.db.iapi.error.StandardException A deadlock has occured (message id will be LockFactory.Deadlock)
		@exception com.splicemachine.db.iapi.error.StandardException The wait for the lock timed out (message id will be LockFactory.TimeOut).
		@exception com.splicemachine.db.iapi.error.StandardException Another thread interupted this thread while
		it was waiting for the lock. This will be a StandardException with a nested java.lang.InterruptedException exception,
		(message id will be LockFactory.InterruptedExceptionId)
		@exception StandardException Standard Derby error policy.

	*/
	boolean zeroDurationlockObject(CompatibilitySpace compatibilitySpace,
								   Lockable ref, Object qualifier,
								   int timeout)
		throws StandardException;

	/**
		Check to see if a specific lock is held.
	*/
	boolean isLockHeld(CompatibilitySpace compatibilitySpace,
					   Object group, Lockable ref, Object qualifier);

	/**
		Get the lock timeout in milliseconds. A negative number means that
        there is no timeout.
	*/
	int getWaitTimeout();

	/**
		Install a limit that is called when the size of the group exceeds
		the required limit.
		<BR>
		It is not guaranteed that the callback method (Limit.reached) is
		called as soon as the group size exceeds the given limit.
		If the callback method does not result in a decrease in the
		number of locks held then the lock factory implementation
		may delay calling the method again. E.g. with a limit
		of 500 and a reached() method that does nothing, may result
		in the call back method only being called when the group size reaches
		550.
		<BR>
		Only one limit may be in place for a group at any time.
		@see Limit
	*/
	void setLimit(CompatibilitySpace compatibilitySpace, Object group,
				  int limit, Limit callback);

	/**
		Clear a limit set by setLimit.
	*/
	void clearLimit(CompatibilitySpace compatibilitySpace, Object group);

	/**
		Make a virtual lock table for diagnostics.
	 */
	Enumeration makeVirtualLockTable();

}


