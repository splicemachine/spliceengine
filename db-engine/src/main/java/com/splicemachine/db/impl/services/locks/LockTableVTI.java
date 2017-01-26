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

package com.splicemachine.db.impl.services.locks;

import com.splicemachine.db.iapi.services.locks.Latch;

import java.util.Enumeration;
import java.util.NoSuchElementException;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.List;
import java.util.Map;

/**
	This provides an Enumeration of Latch's
	from a clone of the lock table. A Latch is badly named,
	it represents lock information.
 */
class LockTableVTI implements Enumeration
{
	// the clonedLockTable temporarily holds a copy of the lock table.
	//
	// The copy is necessary because the real lock manager needs to be single
	// threaded while a snap shot is made.  After the copy is made, it can take
	// its time digesting the information without blocking the real lock
	// manager.

	private final Iterator outerControl;
	private Control control;
	private ListIterator grantedList;
	private ListIterator waitingList;
	private Latch nextLock;

	LockTableVTI(Map clonedLockTable)
	{
		outerControl = clonedLockTable.values().iterator();
	}


	public boolean hasMoreElements() {

		if (nextLock != null)
			return true;

		for (;;) {

			if (control == null) {
				if (!outerControl.hasNext())
					return false;
//System.out.println("new control lock ");

				control = (Control) outerControl.next();

				List granted = control.getGranted();
				if (granted != null)
					grantedList = granted.listIterator();


				List waiting = control.getWaiting();
				if (waiting != null)
					waitingList = waiting.listIterator();

				nextLock = control.getFirstGrant();
				if (nextLock == null) {

					nextLock = getNextLock(control);
				}
				
			} else {
				nextLock = getNextLock(control);
			}


			if (nextLock != null)
				return true;

			control = null;
		}
	}

	private Latch getNextLock(Control control) {
		Latch lock = null;
		if (grantedList != null) {
			if (grantedList.hasNext()) {
				lock = (Latch) grantedList.next();
			}
			else
				grantedList = null;
		}

		if (lock == null) {
			if (waitingList != null) {
				if (waitingList.hasNext()) {
					lock = (Latch) waitingList.next();
				}
				else
					waitingList = null;
			}
		}

		return lock;
	}

	public Object nextElement() {

		if (!hasMoreElements())
			throw new NoSuchElementException();

		Latch ret = nextLock;

		nextLock = null;
		return ret;
	}
}



