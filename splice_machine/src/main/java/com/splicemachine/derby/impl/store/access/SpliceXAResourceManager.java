package com.splicemachine.derby.impl.store.access;

import javax.transaction.xa.Xid;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.store.access.xa.XAResourceManager;

public class SpliceXAResourceManager implements XAResourceManager {

	public SpliceXAResourceManager() {
		
	}
	
	@Override
	public void commit(ContextManager cm, Xid xid, boolean onePhase)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ContextManager find(Xid xid) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void forget(ContextManager cm, Xid xid) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Xid[] recover(int flags) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rollback(ContextManager cm, Xid xid) throws StandardException {
		// TODO Auto-generated method stub
		
	}

}
