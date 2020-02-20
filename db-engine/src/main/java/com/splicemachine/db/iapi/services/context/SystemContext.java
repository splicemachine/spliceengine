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

package com.splicemachine.db.iapi.services.context;

import com.splicemachine.db.iapi.error.ShutdownException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
/**
	A context that shuts the system down if it gets an StandardException
	with a severity greater than or equal to ExceptionSeverity.SYSTEM_SEVERITY
	or an exception that is not a StandardException.
*/
final class SystemContext extends ContextImpl
{
	SystemContext(ContextManager cm) {
		super(cm, "SystemContext");
	}

	public void cleanupOnError(Throwable t) {

		boolean doShutdown = false;
		if (t instanceof StandardException) {
			StandardException se = (StandardException) t;
			int severity = se.getSeverity();
			if (severity < ExceptionSeverity.SESSION_SEVERITY)
				return;
            
            popMe();

			if (severity >= ExceptionSeverity.SYSTEM_SEVERITY)
				doShutdown = true;
		} else if (t instanceof ShutdownException) {
			// system is already shutting down ...
		} else if (t instanceof ThreadDeath) {
			// ignore this too, it means we explicitly told thread to
			// stop.  one way this can happen is after monitor
			// shutdown, so we don't need to shut down again
		}
		
		if (!doShutdown) {
			//ContextManager cm = getContextManager();
			// need to remove me from the list of all contexts.
			ContextService.getService().removeContextManager(getContextManager());
			return;
		}


		try {
			// try to print out that the shutdown is occurring.
			// REVISIT: does this need to be a localizable message?
			System.err.println("Shutting down due to severe error.");
			Monitor.getStream().printlnWithHeader("Shutting down due to severe error." + t.getMessage());

		} finally {
			// we need this to happen even if we fail to print out a notice
			Monitor.getMonitor().shutdown();
		}

	}

}

