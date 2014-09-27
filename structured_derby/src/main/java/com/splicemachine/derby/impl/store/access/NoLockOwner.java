package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.services.locks.LockOwner;

public class NoLockOwner implements LockOwner {
		public static final NoLockOwner INSTANCE = new NoLockOwner();

		private NoLockOwner(){} //don't waste memory creating me
		public boolean noWait() { return true; }

}
