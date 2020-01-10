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

import com.splicemachine.dbTesting.unitTests.harness.T_Fail;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.*;

/**
	This test implements serviceable for testing.  To facility testing, when
	this object is being serviced, it will synchronize on itself and notity all
	waiters.  Test driver may wait on this object and check timesServiced to
	make sure the background daemon has run.
*/
public class T_Serviceable implements Serviceable
{
	// synchronized on this to look at the number 
	// of times this object has been serviced
	protected int timesServiced;

	// constant for checking
	protected final int timesRequeue;
	protected final boolean onDemandOnly;
	protected final boolean subscribed;

	// use this to unsubscribe
	protected int clientNumber;

	// test enqueueing, t = number of times to requeue
	public T_Serviceable(int t)
	{
		timesServiced = 0;
		timesRequeue = t;
		onDemandOnly = false;	// not looked at
		subscribed = false;
		clientNumber = -1;
	}

	// test subscription 
	public T_Serviceable(boolean onDemandOnly)
	{
		timesServiced = 0;
		timesRequeue = 0;		// not looked at
		this.onDemandOnly = onDemandOnly;
		subscribed = true;
	}

	protected void setClientNumber(int n)
	{
		clientNumber = n;
	}

	protected int getClientNumber()
	{
		return clientNumber;
	}

	/*
	 *  Serviceable interface
	 */
	public synchronized int performWork(ContextManager context) 
	{
		context.toString();	// make sure context manager is not null;

		timesServiced++;
		notifyAll();			// notify anyone waiting for me to be serviced

		if (!subscribed && timesRequeue > timesServiced)
			return Serviceable.REQUEUE;
		else
			return Serviceable.DONE;
	}
	
	public boolean serviceASAP()
	{
		return true;
	}


	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


	/*
	 * test utilities
	 */

	protected synchronized void t_wait(int n)
	{
		try
		{
			while (timesServiced < n)
				wait();
		}
		catch (InterruptedException ie) {}
	}

	protected synchronized void t_check(int n) throws T_Fail
	{
		if (timesServiced != n)
			throw T_Fail.testFailMsg("Expect to be serviced " + n + " times, instead serviced " + timesServiced);
	}

}
