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
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.locks.C_LockFactory;
import com.splicemachine.db.iapi.services.locks.Lockable;
import com.splicemachine.db.iapi.services.locks.Limit;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.daemon.Serviceable;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.Matchable;
import com.splicemachine.db.iapi.reference.Property;

import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.locks.LockOwner;

/**
 * An abstract implementation of LockFactory that allows different
 * implementations of the lock table to be plugged in. All the methods of
 * <code>LockFactory</code> are implemented. Subclasses must implement the
 * <code>createLockTable()</code> method and make it return the desired
 * <code>LockTable</code> object.
 *
 * <BR> MT - Mutable - Container Object : Thread Aware
 */
abstract class AbstractPool implements LockFactory
{
	/**
		The complete set of locks in the system

		<BR>
		MT - immutable - content dynamic : LockSet is ThreadSafe
	*/
	protected final LockTable lockTable;

	/**
		True if all deadlocks errors should be logged.
	*/
	int deadlockMonitor;

	protected AbstractPool() {
		lockTable = createLockTable();
	}

	/**
	 * Create the lock table that contains the mapping from
	 * <code>Lockable</code>s to locks.
	 *
	 * @return an object implementing the <code>LockTable</code> interface
	 */
	protected abstract LockTable createLockTable();

	/*
	** Methods of LockFactory
	*/

	/**
		Lock a specific object with a timeout.

		<BR>
		MT - thread safe

		@exception StandardException Standard Derby error policy

		@see LockFactory#lockObject
	*/
	public boolean lockObject(CompatibilitySpace compatibilitySpace,
								Object group,
								Lockable ref, Object qualifier, int timeout)
			throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {

				D_LockControl.debugLock("Lock Request before Grant: ", 
                    compatibilitySpace, group, ref, qualifier, timeout);

                if (SanityManager.DEBUG_ON(Constants.LOCK_STACK_TRACE))
                {
                    // The following will print the stack trace of the lock
                    // request to the log.  
                    Throwable t = new Throwable();
                   java.io.PrintWriter istream = SanityManager.GET_DEBUG_STREAM();

                    istream.println("Stack trace of lock request:");
                    t.printStackTrace(istream);
                }
			}
		}

		Lock lock = 
            lockTable.lockObject(compatibilitySpace, ref, qualifier, timeout);

		// See if NO_WAIT was passed in and the lock could not be granted.
		if (lock == null) {
			if (SanityManager.DEBUG) {
                SanityManager.ASSERT(noLockWait(timeout, compatibilitySpace),
                                     "timeout not NO_WAIT");
			}

            // If this is a timed wait, we should behave as if we timed out
            // and throw a LOCK_TIMEOUT.
            if (timeout == C_LockFactory.TIMED_WAIT) {
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }

			return false;
		}

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock(
                    "Lock Request Granted: ", 
                    compatibilitySpace, group, ref, qualifier, timeout);
			}
		}

		((LockSpace) compatibilitySpace).addLock(group, lock);

		return true;
	}

	/**
	 * Create an object which can be used as a compatibility space within this
	 * lock manager.
	 *
	 * @param owner the owner of the compatibility space
	 * @return an object which represents a compatibility space
	 */
	public CompatibilitySpace createCompatibilitySpace(LockOwner owner) {
		return new LockSpace(owner);
	}

	/**
		Unlock a specific object

		<BR>
		MT - thread safe

		@see LockFactory#unlock
	*/

	public int unlock(CompatibilitySpace compatibilitySpace, Object group,
					  Lockable ref, Object qualifier)
	{
		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock: ", 
                    compatibilitySpace, group, ref, qualifier, -1);
			}
		}

		int count =
			((LockSpace) compatibilitySpace).unlockReference(
				lockTable, ref, qualifier, group);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
                (count == 0) || (count == 1), "count = " + count);
		}

		return count;
	}

	/**
		Unlock a group of objects. 

		<BR>
		MT - thread safe

		@param group handle of group that objects were locked with.
		If group is	null then this call is equivilent to unlockAll().

		@see LockFactory#unlockGroup
	*/
	public void unlockGroup(CompatibilitySpace compatibilitySpace,
							Object group) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock Group: ",
										compatibilitySpace, group);
			}
		}

		((LockSpace) compatibilitySpace).unlockGroup(lockTable, group);
	}

	public void unlockGroup(CompatibilitySpace compatibilitySpace, Object group,
							Matchable key) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				D_LockControl.debugLock("Lock Unlock Group: ",
										compatibilitySpace, group);
			}
		}

		((LockSpace) compatibilitySpace).unlockGroup(lockTable, group, key);
	}

	/**
		Transfer a set of locks from one group to another.

  		<BR>
		MT - thread safe

		@see LockFactory#transfer
	*/
	public void transfer(CompatibilitySpace compatibilitySpace, Object oldGroup,
						 Object newGroup) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE)) {
				StringBuffer sb = new StringBuffer("Lock Transfer:");

				D_LockControl.debugAppendObject(
                    sb, " CompatibilitySpace=", compatibilitySpace);
				D_LockControl.debugAppendObject(sb, " Old Group=", oldGroup);
				D_LockControl.debugAppendObject(sb, " New Group=", newGroup);

				D_LockControl.debugAddThreadInfo(sb);

				SanityManager.DEBUG(Constants.LOCK_TRACE, sb.toString());
			}
		}

		((LockSpace) compatibilitySpace).transfer(oldGroup, newGroup);
	}

	/**
		Returns true if locks by anyone are blocking anyone else
	*/
	public boolean anyoneBlocked() {
		return lockTable.anyoneBlocked();
	}

	/**
		Return true if locks are held in this group and this space.

		<BR>
		MT - thread safe

		@param group handle of group that objects were locked with.

		@see LockFactory#areLocksHeld
	*/
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace,
								Object group) {
		return ((LockSpace) compatibilitySpace).areLocksHeld(group);
	}

	/**
		Return true if locks are held in this space
		
		<BR>
		MT - thread safe

		@see LockFactory#areLocksHeld
	*/
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace) {
		return ((LockSpace) compatibilitySpace).areLocksHeld();
	}

	public boolean zeroDurationlockObject(CompatibilitySpace compatibilitySpace,
										  Lockable ref, Object qualifier,
										  int timeout)
		throws StandardException {
		boolean success = lockTable.zeroDurationLockObject(
			compatibilitySpace, ref, qualifier, timeout);

        if (!success) {

            // zeroDurationLockObject should only return false if we have
            // requested that we shouldn't wait for locks. Otherwise, an
            // exception should have been thrown.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(noLockWait(timeout, compatibilitySpace),
                                     "Should have timed out");
            }

            // If this is a timed wait, we should behave as if we timed out and
            // throw LOCK_TIMEOUT.
            if (timeout == C_LockFactory.TIMED_WAIT) {
                throw StandardException.newException(SQLState.LOCK_TIMEOUT);
            }
        }

        return success;
	}

	public boolean isLockHeld(CompatibilitySpace compatibilitySpace,
							  Object group, Lockable ref, Object qualifier) {
		return ((LockSpace) compatibilitySpace).isLockHeld(
			group, ref, qualifier);
	}

	/**
		Get the lock timeout in milliseconds. A negative number means that
        there is no timeout.
	*/
	public int getWaitTimeout() { return lockTable.getWaitTimeout(); }

	public void setLimit(CompatibilitySpace compatibilitySpace,
						 Object group, int limit, Limit callback) {
		((LockSpace) compatibilitySpace).setLimit(group, limit, callback);
	}

	/**
		Clear a limit set by setLimit.
	*/
	public void clearLimit(CompatibilitySpace compatibilitySpace, Object group)
	{
		((LockSpace) compatibilitySpace).clearLimit(group);
	}

    /**
     * Check if we should not wait for locks, given the specified timeout and
     * compatibility space. If the timeout is {@code C_LockFactory.NO_WAIT} or
     * the {@code LockOwner} has the {@code noWait} flag set, we shouldn't
     * wait for locks.
     *
     * @param timeout the specified timeout
     * @param compat the compatibility space
     * @return {@code true} if we shouldn't wait for locks, {@code false}
     * otherwise
     */
    static boolean noLockWait(int timeout, CompatibilitySpace compat) {
        if (timeout == C_LockFactory.NO_WAIT) {
            return true;
        } else {
            LockOwner owner = compat.getOwner();
            return owner != null && owner.noWait();
        }
    }

//EXCLUDE-START-lockdiag- 

	/**
		Routines to support lock diagnostics VTIs for the benefit of VirtualLockTable
	 */
	/* package */
	public Enumeration makeVirtualLockTable()
	{
		// make a shallow copy of the locktable.

		return new LockTableVTI(lockTable.shallowClone());
	}
//EXCLUDE-END-lockdiag- 


	/*
	** Methods of PropertySetCallback
	*/

	public void init(boolean dbOnly, Dictionary p) {

		getAndApply(dbOnly, p, Property.DEADLOCK_TIMEOUT);
		getAndApply(dbOnly, p, Property.LOCKWAIT_TIMEOUT);
		getAndApply(dbOnly, p, Property.DEADLOCK_MONITOR);
//EXCLUDE-START-lockdiag- 
        getAndApply(dbOnly, p, Property.DEADLOCK_TRACE);
//EXCLUDE-END-lockdiag- 
	}

	private void getAndApply(boolean dbOnly, Dictionary p, String key) {

		try {

			Serializable value = (Serializable) PropertyUtil.getPropertyFromSet(dbOnly, p, key);
			if (value != null) {
				validate(key, value, p);
				apply(key, value, p,null);
			}
		} catch (StandardException se) {
			// just ignore value at bootup.
		}
	}

	
	public boolean validate(String key, Serializable value, Dictionary p)
		throws StandardException {

		if (!key.startsWith(Property.LOCKS_INTRO))
			return false;

		if (value != null) {

			if (key.equals(Property.DEADLOCK_TIMEOUT))
				getWaitValue((String) value,  Property.DEADLOCK_TIMEOUT_DEFAULT);
			else if (key.equals(Property.LOCKWAIT_TIMEOUT))
				getWaitValue((String) value,  Property.WAIT_TIMEOUT_DEFAULT);
			else if (key.equals(Property.DEADLOCK_MONITOR))
				PropertyUtil.booleanProperty(Property.DEADLOCK_MONITOR, value, false);
            else if (key.equals(Property.DEADLOCK_TRACE))
                PropertyUtil.booleanProperty(Property.DEADLOCK_TRACE, value, false);
		}

		return true;
	}

	@Override
    public Serviceable apply(String key, Serializable value, Dictionary p, TransactionController tc)
		throws StandardException {

		if (value == null) {
			// a delete, fill in the new value
			value = PropertyUtil.getPropertyFromSet(p, key);
		}

		String svalue = (String) value;

		if (key.equals(Property.DEADLOCK_TIMEOUT))
			lockTable.setDeadlockTimeout(
				getWaitValue(svalue,  Property.DEADLOCK_TIMEOUT_DEFAULT));
		else if (key.equals(Property.LOCKWAIT_TIMEOUT))
			lockTable.setWaitTimeout(
				getWaitValue(svalue,  Property.WAIT_TIMEOUT_DEFAULT));
		else if (key.equals(Property.DEADLOCK_MONITOR)) {
			deadlockMonitor = PropertyUtil.booleanProperty(Property.DEADLOCK_MONITOR, svalue, false) ?
				StandardException.REPORT_ALWAYS : StandardException.REPORT_DEFAULT;
		}
//EXCLUDE-START-lockdiag- 
        else if (key.equals(Property.DEADLOCK_TRACE))
            lockTable.setDeadlockTrace(PropertyUtil.booleanProperty(Property.DEADLOCK_TRACE, svalue, false));
//EXCLUDE-END-lockdiag- 

		return null;
	}
	
    public Serializable map(String key, Serializable value, Dictionary p) {
		return null;
	}

	/*
	** Property related methods
	*/
	
	private static int getWaitValue(String value, int defaultValue ) {

		// properties are defined in seconds
		int wait = PropertyUtil.handleInt(value, Integer.MIN_VALUE, Integer.MAX_VALUE / 1000, defaultValue);

		if (wait < 0)
			wait = C_LockFactory.WAIT_FOREVER;
		else
			// convert to milliseconds
			wait *= 1000;

		return wait;
	}
}
