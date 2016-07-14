/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
