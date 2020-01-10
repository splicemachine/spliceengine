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

package com.splicemachine.dbTesting.unitTests.services;

import com.splicemachine.db.iapi.reference.Module;
import com.splicemachine.dbTesting.unitTests.harness.T_Generic;
import com.splicemachine.dbTesting.unitTests.harness.T_Fail;

import com.splicemachine.db.iapi.services.cache.*;

import com.splicemachine.db.iapi.services.daemon.*;

import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.error.StandardException;

public class T_CacheService extends T_Generic implements CacheableFactory {

	protected CacheFactory	cf;

	public Cacheable newCacheable(CacheManager cm) {
		return new T_CachedInteger();
	}

	/**
		@exception T_Fail - the test has failed.
	*/
	protected void runTests() throws T_Fail {

		DaemonFactory df;
		try {
			cf = (CacheFactory) Monitor.startSystemModule(getModuleToTestProtocolName());
			df = (DaemonFactory) Monitor.startSystemModule(Module.DaemonFactory);
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}
		if (cf == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " module not started.");
		}
		if (df == null)
			throw T_Fail.testFailMsg(Module.DaemonFactory + " module not started.");
	

		try {

			DaemonService ds = df.createNewDaemon("CacheTester");
			if (ds == null)
				throw T_Fail.testFailMsg("Can't create deamon service");

			CacheManager cm1 = cf.newCacheManager(this, "testCache1", 20, 40);
			if (cm1 == null)
				throw T_Fail.testFailMsg("unable to create cache manager");
			T001(cm1, 40);
			cm1.useDaemonService(ds);
			thrashCache(cm1, 10, 1000);
			cm1.shutdown();
			cm1 = null;

			CacheManager cm2 = cf.newCacheManager(this, "testCache2", 0, 1);
			if (cm2 == null)
				throw T_Fail.testFailMsg("unable to create cache manager");
			T001(cm2, 1);
			cm2.useDaemonService(ds);
			thrashCache(cm2, 10, 1000);
			cm2.shutdown();
			cm2 = null;

			CacheManager cm3= cf.newCacheManager(this, "testCache3", 2000, 40);
			if (cm3 == null)
				throw T_Fail.testFailMsg("unable to create cache manager");
			T001(cm3, 40);
			cm3.useDaemonService(ds);
			thrashCache(cm3, 10, 1000);
			cm3.shutdown();
			cm3 = null;

			// now two that don't use the daemon service
			CacheManager cm4 = cf.newCacheManager(this, "testCache4", 2000, 40);
			if (cm4 == null)
				throw T_Fail.testFailMsg("unable to create cache manager");
			T001(cm4, 40);
			thrashCache(cm4, 10, 1000);
			cm4.shutdown();
			cm4 = null;

			CacheManager cm5 = cf.newCacheManager(this, "testCache5", 0, 40);
			if (cm5 == null)
				throw T_Fail.testFailMsg("unable to create cache manager");
			T001(cm5, 40);
			thrashCache(cm5, 10, 1000);
			cm5.shutdown();
			cm5 = null;

		} catch (StandardException se) {
			throw T_Fail.exceptionFail(se);
		} catch (Throwable t) {
			t.printStackTrace();
			throw T_Fail.exceptionFail(t);	
		}
	}

	/**
	  Get the name of the protocol for the module to test.
	  This is the 'factory.MODULE' variable.
	  
	  'moduleName' to the name of the module to test. 

	  */
	protected String getModuleToTestProtocolName() {
		return Module.CacheFactory;
	}


	/*
	** The tests
	*/

	/**
		Test the find and findCached calls.
		@exception StandardException  Standard Derby Error policy
		@exception T_Fail  Some error
	*/
	protected void T001(CacheManager cm, int cacheSize) throws T_Fail, StandardException {

		T_Key tkey1 = T_Key.simpleInt(1);

		// cahce is empty, nothing should be there
		t_findCachedFail(cm, tkey1);

		// find a valid entry
		cm.release(t_findSucceed(cm, tkey1));

		// check it is still in the cache
		cm.release(t_findCachedSucceed(cm, tkey1));

		// look for an item that can't be found
		T_Key tkey2 = T_Key.dontFindInt(2);
		t_findCachedFail(cm, tkey2);
		t_findFail(cm, tkey2);

		// see if the first item still can be found
		// can't assume it can be cached as it may have aged out ...
		cm.release(t_findSucceed(cm, tkey1));

		// now ensure we can find an item with the key that just couldn't
		// be found
		tkey2 = T_Key.simpleInt(2);
		cm.release(t_findSucceed(cm, tkey2));
		cm.release(t_findSucceed(cm, tkey1));


		// now create a key that will cause an exception ...
		T_Key tkey3 = T_Key.exceptionInt(3);
		t_findCachedFail(cm, tkey3);
		try {
			
			t_findFail(cm, tkey3);
			throw T_Fail.testFailMsg("find call lost user exception");
		} catch (StandardException se) {
			if (!(se instanceof T_CacheException))
				throw se;
			if (((T_CacheException) se).getType() != T_CacheException.IDENTITY_FAIL)
				throw se;
		}

		tkey3 = T_Key.simpleInt(3);
		cm.release(t_findSucceed(cm, tkey3));
		cm.release(t_findSucceed(cm, tkey2));
		cm.release(t_findSucceed(cm, tkey1));

		// since this cache is in use by only this method we should
		// be able to call clean with deadlocking and then ageOut
		// leaving the cache empty.
		cm.cleanAll();
		cm.ageOut();

		t_findCachedFail(cm, tkey1);
		t_findCachedFail(cm, tkey2);
		t_findCachedFail(cm, tkey3);


		// now put many valid objects into the cache
		for (int i = 0; i < 4 * cacheSize ; i++) {
			T_Key tkeyi = T_Key.simpleInt(i);
			cm.release(t_findSucceed(cm, tkeyi));
			cm.release(t_findCachedSucceed(cm, tkeyi));
		}
		cm.cleanAll();
		cm.ageOut();
		for (int i = 0; i < 4 * cacheSize ; i++) {
			T_Key tkeyi = T_Key.simpleInt(i);
			t_findCachedFail(cm, tkeyi);
		}


		// Ensure that we can find an object multiple times
		Cacheable e1 = t_findSucceed(cm, tkey1);
		Cacheable e2 = t_findSucceed(cm, tkey2);

		if (e1 == e2)
			throw T_Fail.testFailMsg("same object returned for two different keys");

		if (t_findSucceed(cm, tkey1) != e1)
			throw T_Fail.testFailMsg("different object returned for same key");
		if (t_findSucceed(cm, tkey2) != e2)
			throw T_Fail.testFailMsg("different object returned for same key");

		cm.release(e1);
		cm.release(e1);
		e1 = null;
		cm.release(e2);
		cm.release(e2);
		e2 = null;



		
		PASS("T001");
	}




	/*
	** Multi-user tests
	*/


	protected void thrashCache(CacheManager cm, int threads, int iterations) throws T_Fail {

		Thread[] children = new Thread[threads];

		for (int i = 0; i < threads; i++) {

			children[i] = new Thread(new T_CacheUser(cm, iterations, this, out));
			
		}

		for (int i = 0; i < threads; i++) {
			children[i].start();
		}

		try {
			for (int i = 0; i < threads; i++) {
				if (threadFail != null)
					throw threadFail;

				children[i].join();

				if (threadFail != null)
					throw threadFail;
			}
		} catch (InterruptedException ie) {
			throw T_Fail.exceptionFail(ie);
		}

		PASS("thrashCache");

	}
	protected T_Fail threadFail;
	public synchronized void setChildException(T_Fail tf) {
		if (threadFail == null)
			threadFail = tf;
	}


	/**
		A call to findCached() that is expected to return nothing.
		@exception StandardException  Standard Derby Error policy
		@exception T_Fail Something was found.
	*/
	protected void t_findCachedFail(CacheManager cm, Object key) throws StandardException, T_Fail {
		Cacheable entry = cm.findCached(key);
		if (entry != null) {
			throw T_Fail.testFailMsg("found cached item unexpectedly");
		}
	}

	/**
		A call to findCached() that is expected to find something.
		@exception StandardException  Standard Derby Error policy
		@exception T_Fail Nothing was found.
	*/
	protected Cacheable t_findCachedSucceed(CacheManager cm, Object key) throws StandardException, T_Fail {
		Cacheable entry = cm.findCached(key);
		if (entry == null) {
			throw T_Fail.testFailMsg("expected item to be in cache");
		}

		if (!entry.getIdentity().equals(key))
			throw T_Fail.testFailMsg("item returned does not match key");
		return entry;
	}
	/**
		A call to find() that is expected to return nothing.

		@exception T_Fail Something was found.
		@exception StandardException  Standard Derby Error policy
	*/
	protected void t_findFail(CacheManager cm, Object key) throws T_Fail, StandardException {
		Cacheable entry = cm.find(key);
		if (entry != null) {
			throw T_Fail.testFailMsg("found item unexpectedly");
		}
	}

	/**
		A call to findCached() that is expected to find something.

		@exception T_Fail Nothing was found.
		@exception StandardException  Standard Derby Error policy
	*/
	protected Cacheable t_findSucceed(CacheManager cm, Object key) throws T_Fail, StandardException {
		Cacheable entry = cm.find(key);
		if (entry == null) {
			throw T_Fail.testFailMsg("expected item to be found");
		}
		if (!entry.getIdentity().equals(key))
			throw T_Fail.testFailMsg("item returned does not match key");

		return entry;
	}
}
