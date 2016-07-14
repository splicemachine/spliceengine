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

package com.splicemachine.si.api.txn;

/**
 * Interface representing a scheduler which ensures that transactions
 * are kept-alive so long as the following conditions are met:
 *
 * 1. The transaction is not in a known terminal state (e.g. that its memory-state
 * is ACTIVE)
 * 2. the Protocol indicates that the keep-alive should continue
 * 3. this scheduler is still running.
 *
 * @author Scott Fines
 * Date: 6/25/14
 */
public interface KeepAliveScheduler {

		/**
		 * Schedule the transaction for keep alive.
		 *
		 * If the transaction is in a known terminal state (e.g. if you schedule a committed or rolled back
		 * transaction) at submission time, then this will not schedule it further.
		 *
		 * If the scheduler is shut down, then this method will perform no action.
		 *
		 * @param txn the transaction to keep alive.
		 */
		void scheduleKeepAlive(Txn txn);

		/**
		 * Start the scheduler, including any resources associated with it.
 		 */
		void start();

		/**
		 * Stop the scheduler. Future keep-alive scheduling requests will be ignored.
		 */
		void stop();
}
