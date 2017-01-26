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
