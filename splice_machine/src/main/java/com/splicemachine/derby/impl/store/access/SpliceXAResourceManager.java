/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
