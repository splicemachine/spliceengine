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
