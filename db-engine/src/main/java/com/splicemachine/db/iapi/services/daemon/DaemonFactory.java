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

import com.splicemachine.db.iapi.error.StandardException;

/**
	Daemon Factory can create new DaemonService, which runs on seperate
	background threads.  One can use these DaemonService to handle background
	clean up task by implementing Serviceable and subscribing to a DaemonService.

	A DaemonService is a background worker thread which does asynchronous I/O and
	general clean up.  It should not be used as a general worker thread for
	parallel execution.  A DaemonService can be subscribe to by many Serviceable
	objects and a daemon will call that object's performWork from time to
	time.  These performWork method should be well behaved - in other words,
	it should not take too long or hog too many resources or deadlock with 
	anyone else.  And it cannot (should not) error out.

	The best way to use a daemon is to have an existing DaemonService and subscribe to it.
	If you can't find an existing one, then make one thusly:

	DaemonService daemon = DaemonFactory.createNewDaemon();

	After you have a daemon, you can subscribe to it by
	int myClientNumber = daemon.subscribe(serviceableObject);

	and ask it to run performWork for you ASAP by
	daemon.serviceNow(myClientNumber);

	Or, for one time service, you can enqueue a Serviceable Object by
	daemon.enqueue(serviceableObject, true);  - urgent service
	daemon.enqueue(serviceableObject, false); - non-urgent service

	@see DaemonService
	@see Serviceable
*/
public interface DaemonFactory 
{
	/**
		Create a new DaemonService with the default daemon timer delay.

		@exception StandardException Standard Derby error policy
	 */
	public DaemonService createNewDaemon(String name) throws StandardException;
}
