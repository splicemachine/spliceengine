/*

   Derby - Class com.splicemachine.db.iapi.store.access.conglomerate.ScanManager

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
