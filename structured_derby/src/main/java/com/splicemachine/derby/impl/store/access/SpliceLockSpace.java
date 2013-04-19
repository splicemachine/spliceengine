package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockOwner;

public class SpliceLockSpace implements CompatibilitySpace {
	
	public LockOwner getOwner() {
		return new SpliceLockOwner();
	}

}
