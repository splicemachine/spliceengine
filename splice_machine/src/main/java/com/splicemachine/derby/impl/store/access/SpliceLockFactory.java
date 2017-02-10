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

import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Properties;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.locks.Limit;
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.locks.LockOwner;
import com.splicemachine.db.iapi.services.locks.Lockable;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.Matchable;
import org.apache.log4j.Logger;

//FIXME: We may need to implement unlock and zeroDurationlockObject since they are called directly by DataDictionaryImpl
//and used by locking the cache.

public class SpliceLockFactory implements LockFactory, ModuleControl, ModuleSupportable {
	private static Logger LOG = Logger.getLogger(SpliceLockFactory.class);
	
	//private TxnLockManager lockManager;
	
	public void init(boolean dbOnly, Dictionary p) {
		if (LOG.isTraceEnabled())
			LOG.trace("init");
	}

	
	public boolean validate(String key, Serializable value, Dictionary p)
			throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("validate");
		return false;
	}

	
	public Serviceable apply(String key, Serializable value, Dictionary p, TransactionController tc)
			throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("apply");
		return null;
	}

	
	public Serializable map(String key, Serializable value, Dictionary p)
			throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("map");
		return null;
	}

	
	public CompatibilitySpace createCompatibilitySpace(LockOwner owner) {
		if (LOG.isTraceEnabled())
			LOG.trace("createCompatibilitySpace");
		return null;
	}

	
	public boolean lockObject(CompatibilitySpace compatibilitySpace,
			Object group, Lockable ref, Object qualifier, int timeout)
			throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("lockObject");
		return true;
	}

	
	public int unlock(CompatibilitySpace compatibilitySpace, Object group,
			Lockable ref, Object qualifier) {
		if (LOG.isTraceEnabled())
			LOG.trace("unlock");
		return 1;
	}

	
	public void unlockGroup(CompatibilitySpace compatibilitySpace, Object group) {
		if (LOG.isTraceEnabled())
			LOG.trace("unlockGroup");
		
	}

	
	public void unlockGroup(CompatibilitySpace compatibilitySpace,
			Object group, Matchable key) {
		if (LOG.isTraceEnabled())
			LOG.trace("unlockGroup");		
	}

	
	public void transfer(CompatibilitySpace compatibilitySpace,
			Object oldGroup, Object newGroup) {
		if (LOG.isTraceEnabled())
			LOG.trace("transfer");

		
	}

	
	public boolean anyoneBlocked() {
		if (LOG.isTraceEnabled())
			LOG.trace("anyoneBlocked");
		return false;
	}

	
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace,
			Object group) {
		if (LOG.isTraceEnabled())
			LOG.trace("areLocksHeld");
		return false;
	}

	
	public boolean areLocksHeld(CompatibilitySpace compatibilitySpace) {
		// TODO Auto-generated method stub
		if (LOG.isTraceEnabled())
			LOG.trace("areLocksHeld");
		return false;
	}

	
	public boolean zeroDurationlockObject(
			CompatibilitySpace compatibilitySpace, Lockable ref,
			Object qualifier, int timeout) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("zeroDurationlockObject");
		return true;
	}

	
	public boolean isLockHeld(CompatibilitySpace compatibilitySpace,
			Object group, Lockable ref, Object qualifier) {
		if (LOG.isTraceEnabled())
			LOG.trace("isLockHeld");
		return false;
	}

	
	public int getWaitTimeout() {
		if (LOG.isTraceEnabled())
			LOG.trace("getWaitTimeout");
		return 0;
	}

	
	public void setLimit(CompatibilitySpace compatibilitySpace, Object group,
			int limit, Limit callback) {
		if (LOG.isTraceEnabled())
			LOG.trace("setLimit");
		
	}

	
	public void clearLimit(CompatibilitySpace compatibilitySpace, Object group) {
		if (LOG.isTraceEnabled())
			LOG.trace("clearLimit");
		
	}

	
	public Enumeration makeVirtualLockTable() {
		if (LOG.isTraceEnabled())
			LOG.trace("makeVirtualLockTable");
		return null;
	}

	
	public boolean canSupport(Properties properties) {
		// TODO Auto-generated method stub
		return false;
	}

	
	public void boot(boolean create, Properties properties)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
