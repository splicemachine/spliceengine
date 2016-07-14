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

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Represents a "Committed transaction"--that is,
 * a transaction which when read was already committed.
 *
 * This is primarily useful for constructing stub transactions
 * when performing scans over commit timestamp columns.
 *
 * @author Scott Fines
 * Date: 6/23/14
 */
public class CommittedTxn extends InheritingTxnView {
		@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
		public CommittedTxn(long beginTimestamp, long endTimestamp){
			super(Txn.ROOT_TRANSACTION,beginTimestamp,beginTimestamp,Txn.ROOT_TRANSACTION.getIsolationLevel(),false,false,false,false,endTimestamp,endTimestamp, Txn.State.COMMITTED);
		}
}
