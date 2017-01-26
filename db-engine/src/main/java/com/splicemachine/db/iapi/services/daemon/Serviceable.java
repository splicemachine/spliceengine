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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.services.daemon;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.error.StandardException;

/**
  To use a DaemonService, one implements the Serviceable interface.  Only one
  DaemonService will call this at any given time.  However, if this Serviceable
  object subscribes to or enqueues to the DeamonService multiple times, then
  multiple DaemonService threads may call this Serviceable object at the same
  time.  The Serviceable object must decide what synchronization it needs to
  provide depending on what work it needs to do.

  The Serviceable interface does not provide a way to pass a work object to
  identify what work needs to be done, it is assumed that the Serviceable
  object knows that.  If a Serviceable object has different work for the
  DaemonService to do, it must somehow encapsulate or identify the different
  work by an intermediary object which implements the Serviceable interface and
  which can an identify the different work and pass it along to the object that
  can deal with them.
*/

public interface Serviceable {

	/**
		Do whatever it is that you want the daemon to do for you. There may be
		multiple daemon objects on different thread calling performWork at the
		same time. 

		The DaemonService will always call performWork with a context manager
		set up.  the DaemonService will clean up the context if an exception is
		thrown.  However, it is up to performWork to manage its own
		transaction.  If you start a transaction in performWork, you
		<B>must</B> commit or abort it at the end.  You may leave the
		transaction open so that other serviceable may use the transaction and
		context without starting a new one.  On the same token, there may
		already be an opened transaction on the context.  Serviceable
		performWork should always check the state of the context before use.

		A Serviceable object should be well behaved while it is performing the
		daemon work, i.e., it should not take too many resources or hog the CPU
		for too long or deadlock with anyone else.

		@param context the contextManager set up by the DaemonService.  There
		may or may not be the necessary context on it, depending on which other
		Serviceable object it has done work for.
		@return the return status is only significant if the Serviceable client
		was enqueued instead of subscribed.  For subscribed client, the return
		status is ignored.  For enqueue client, it returns DONE or REQUEUE.  If
		a REQUEUEd is returned, it would be desirable if this should not be
		serviceASAP, although no harm is done if this still maintains that this
		should be serviced ASAP ...

	    @exception StandardException  Standard Derby exception policy

		<P>MT - depends on the work.  Be wary of multiple DaemonService thread
		calling at the same time if you subscribe or enqueue multiple times.
	*/
	public int performWork(ContextManager context) throws StandardException;


	/** return status for performWork - only meaningful for enqueued client */
	public static int DONE = 1;	// the daemon work is finished, the
								// DaemonService can get rid of this client
	public static int REQUEUE = 2;// the daemon work is not finished, requeue
								  // the request to be serviced again later.



	/**
		If this work should be done as soon as possible, then return true.  
		If it doesn't make any difference if it is done sooner rather than
		later, then return false.  

		The difference is whether or not the daemon service will be notified to
		work on this when this work is enqueued or subscribed, in case the
		serviceable work is put together but not sent to the daemon service
		directly, like in post commit processing

		<P>MT - MT safe
	*/
	public boolean serviceASAP();


	/**
		If this work should be done immediately on the user thread then return true.  
		If it doesn't make any difference if this work is done on a the user thread
		immediately or if it is performed by another thread asynchronously
		later, then return false.  
	*/
	public boolean serviceImmediately();

}

