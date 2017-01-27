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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;

/**

This interface allows access to commit,prepare,abort global transactions
as part of a two phase commit protocol, during runtime.  
These interfaces have been chosen to be exact implementations required to 
implement the XAResource interfaces as part of the JTA standard extension.
<P>
It is expected that the following interfaces are only used during the 
runtime portion of a 2 phase commit connection.
<P>
If a runtime exception causes a transaction abort (of a transaction that
has not been successfully prepared), then the transaction will act as if 
xa_rollback() had been called.  The transaction will be aborted and any
other call other than destroy will throw exceptions.
<P>
The XAResource interface is a Java mapping of the industry standard XA resource
manager interface.  Please refer to: X/Open CAE Specification - Distributed 
Transaction Processing: The XA Specification, X/Open Document No. XO/CAE/91/300
or ISBN 1 872630 24 3.
<P>
NOTE - all calls to this interface assume that the caller has insured that
there is no active work being done on the local instance of the transaction 
in question.  RESOLVE - not sure whether this means that the connection 
associated with the transaction must be closed, or if it just means that
synchronization has been provided to provide correct MT behavior from above.

**/

public interface XATransactionController extends TransactionController
{
    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
    public static final int XA_RDONLY = 1;
    public static final int XA_OK     = 2;

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * Once this call has been made all other calls on this controller other
     * than destroy will throw exceptions.
     * <p>
     *
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 current xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_commit(
    boolean onePhase)
		throws StandardException;

    /**
     * This method is called to ask the resource manager to prepare for
     * a transaction commit of the transaction specified in xid.
     * <p>
     * If XA_OK is returned then any call other than xa_commit() or xa_abort()
     * will throw exceptions.  If XA_RDONLY is returned then any call other
     * than destroy() will throw exceptions.
     *
     * @return         A value indicating the resource manager's vote on the
     *                 the outcome of the transaction.  The possible values
     *                 are:  XA_RDONLY or XA_OK.  If the resource manager wants
     *                 to roll back the transaction, it should do so by 
     *                 throwing an appropriate XAException in the prepare
     *                 method.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int xa_prepare()
		throws StandardException;

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     * Once this call has been made all other calls on this controller other
     * than destroy will throw exceptions.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_rollback()
        throws StandardException;
}
