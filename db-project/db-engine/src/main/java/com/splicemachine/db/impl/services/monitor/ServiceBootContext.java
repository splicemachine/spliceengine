/*

   Derby - Class com.splicemachine.db.impl.services.monitor.ServiceBootContext

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.services.monitor;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.error.ExceptionSeverity;

/**
	A context that is used during a service boot to
	stop cleanup on the stack at this point.
*/
final class ServiceBootContext extends ContextImpl {

	ServiceBootContext(ContextManager cm) {
		super(cm, "ServiceBoot");
	}

	public void cleanupOnError(Throwable t) {
		popMe();
	}

	public boolean isLastHandler(int severity)
	{
		return (severity == ExceptionSeverity.NO_APPLICABLE_SEVERITY) ||
			   (severity == ExceptionSeverity.DATABASE_SEVERITY) ||
			   (severity == ExceptionSeverity.SYSTEM_SEVERITY);
	}
}
