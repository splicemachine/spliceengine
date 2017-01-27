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

import com.splicemachine.db.iapi.services.sanity.SanityManager;

/** 

  A DaemonService provides a background service which is suitable for
  asynchronous I/O and general clean up.  It should not be used as a general
  worker thread for parallel execution.  A DaemonService can be subscribe to by
  many Serviceable objects and a DaemonService will call that object's
  performWork from time to time.  The performWork method is defined by the
  client object and should be well behaved - in other words, it should not take
  too long or hog too many resources or deadlock with anyone else.  And it
  cannot (should not) error out.

  <P>It is up to each <code>DaemonService</code> implementation to define its
  level of service, including
  <UL>
  <LI>how quickly and how often the clients should expect to be be serviced
  <LI>how the clients are prioritized
  <LI>whether the clients need to tolerate spurious services
  </UL>
 
  <P>MT - all routines on the interface must be MT-safe.

  @see Serviceable
*/

public interface DaemonService 
{
	public static int TIMER_DELAY = 10000; // wake up once per TIMER_DELAY milli-second


	/**
		Trace flag that can be used by Daemons to print stuff out
	*/
	public static final String DaemonTrace = SanityManager.DEBUG ? "DaemonTrace" : null;

	/**
		Trace flag that can be used to turn off background daemons
		If DaemonOff is set, background Daemon will not attempt to do anything.
	*/
	public static final String DaemonOff = SanityManager.DEBUG ? "DaemonOff" : null;


	/**
		Add a new client that this daemon needs to service

		@param newClient a Serviceable object this daemon will service from time to time
		@param onDemandOnly only service this client when it ask for service with a serviceNow request
		@return a client number that uniquely identifies this client (this subscription) 
	*/
	public int subscribe(Serviceable newClient, boolean onDemandOnly);


	/**
		Get rid of a client from the daemon. If a client is being serviced when
		the call is made, the implementation may choose whether or not the call
		should block until the client has completed its work. If the call does
		not block, the client must be prepared to handle calls to its
		<code>performWork()</code> method even after <code>unsubscribe()</code>
		has returned.

		@param clientNumber the number that uniquely identify the client
	*/
	public void unsubscribe(int clientNumber);


    /**
     * Service this subscription ASAP. When this method is called, the
     * subscriber's <code>performWork()</code> method is guaranteed to be
     * invoked at some point in the future. However, there is no guarantee that
     * a subscriber's <code>performWork()</code> is called the same number of
     * times as the subscriber calls this method. More precisely, if a
     * subscriber is waiting for this daemon service to invoke its
     * <code>performWork()</code> method, the daemon service may, but is not
     * required to, ignore requests from that subscriber until the
     * <code>performWork()</code> method has been invoked.
     *
     * @param clientNumber the number that uniquely identifies the client
     */
	public void serviceNow(int clientNumber);


	/**
		Request a one time service from the Daemon.  Unless performWork returns
		REQUEUE (see Serviceable), the daemon will service this client once
		and then it will get rid of this client.  Since no client number is
		associated with this client, it cannot request to be serviced or be
		unsubscribed. 

		The work is always added to the deamon, regardless of the
		state it returns.

		@param newClient the object that needs a one time service

		@param serviceNow if true, this client should be serviced ASAP, as if a
		serviceNow has been issued.  If false, then this client will be
		serviced with the normal scheduled.

		@return true if the daemon indicates it is being overloaded,
		false it's happy.
	*/
	public boolean enqueue(Serviceable newClient, boolean serviceNow);

	/**
		Pause.  No new service is performed until a resume is issued.
	*/
	public void pause();
	

	/**
		Resume service after a pause
	*/
	public void resume();
	

	/**
		End this daemon service
	 */
	public void stop();

	/**
		Clear all the queued up work from this daemon.  Subscriptions are not
		affected. 
	 */
	public void clear();

	/*
	 *Wait until work in the high priorty queue is done.
	 */	
	public void waitUntilQueueIsEmpty();
	
}

