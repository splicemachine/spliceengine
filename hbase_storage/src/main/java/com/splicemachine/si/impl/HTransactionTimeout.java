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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a Txn has timed out,
 * and a specific action cannot be taken
 * @author Scott Fines
 * Date: 6/25/14
 */
public class HTransactionTimeout extends DoNotRetryIOException implements TransactionTimeoutException{
		public HTransactionTimeout() {
		}
		public HTransactionTimeout(long txnId) {
				super("Txn "+ txnId+" has timed out");
		}

		public HTransactionTimeout(String message) {
				super(message);
		}

		public HTransactionTimeout(String message,Throwable cause) {
				super(message, cause);
		}
}
