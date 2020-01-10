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

package com.splicemachine.db.iapi.store.access.conglomerate;

import com.splicemachine.db.iapi.store.access.GroupFetchScanController;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.error.StandardException;

/**

The ScanManager interface contains those methods private to access method
implementors necessary to implement Scans on Conglomerates.  Client of scans
use the ScanController to interact with the scan.
<P>
@see ScanController

**/

public interface ScanManager extends ScanController, GroupFetchScanController
{

    /**
     * Close scan as part of terminating a transaction.
     * <p>
     * Use this call to close the scan resources as part of committing or
     * aborting a transaction.  The normal close() routine may do some cleanup
     * that is either unnecessary, or not correct due to the unknown condition
     * of the scan following a transaction ending error.  Use this call when
     * closing all scans as part of an abort of a transaction.
     *
     * @param closeHeldScan     If true, means to close scan even if it has been
     *                          opened to be kept opened across commit.  This is
     *                          used to close these scans on abort.
     *
	 * @return boolean indicating that the close has resulted in a real close
     *                 of the scan.  A held scan will return false if called 
     *                 by closeForEndTransaction(false), otherwise it will 
     *                 return true.  A non-held scan will always return true.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    boolean closeForEndTransaction(boolean closeHeldScan)
		throws StandardException;

}
