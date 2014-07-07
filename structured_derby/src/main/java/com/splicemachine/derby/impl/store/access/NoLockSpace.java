package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockOwner;

public class NoLockSpace implements CompatibilitySpace {
		public static final NoLockSpace INSTANCE = new NoLockSpace();

		private NoLockSpace(){} //don't waste memory creating me
		public LockOwner getOwner() { return NoLockOwner.INSTANCE; }

}
