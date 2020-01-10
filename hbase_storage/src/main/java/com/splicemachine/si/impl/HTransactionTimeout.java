/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a Transaction has timed out,
 * and a specific action cannot be taken
 * @author Scott Fines
 * Date: 6/25/14
 */
public class HTransactionTimeout extends DoNotRetryIOException implements TransactionTimeoutException{
		public HTransactionTimeout() {
		}
		public HTransactionTimeout(long txnId) {
				super("Transaction "+ txnId+" has timed out");
		}

		public HTransactionTimeout(String message) {
				super(message);
		}

		public HTransactionTimeout(String message,Throwable cause) {
				super(message, cause);
		}
}
