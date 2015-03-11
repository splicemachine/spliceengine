package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.locks.LockOwner;

public class NoLockSpace implements CompatibilitySpace {
		public static final NoLockSpace INSTANCE = new NoLockSpace();

		private NoLockSpace(){} //don't waste memory creating me
		public LockOwner getOwner() { return NoLockOwner.INSTANCE; }

}
