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
 * @author Scott Fines
 *         Date: 6/23/14
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
public class RolledBackTxn extends InheritingTxnView {

		public RolledBackTxn(long txnId){
			super(Txn.ROOT_TRANSACTION,txnId,txnId,null,false,false,false,false,-1l,-1l, Txn.State.ROLLEDBACK);
		}
}
