/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.store.access.conglomerate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.SortCostController;

/**

  The factory interface for all sort access methods.

**/

public interface SortFactory extends MethodFactory
{
	/**
	Used to identify this interface when finding it with the Monitor.
	**/
	public static final String MODULE = 
	  "com.splicemachine.db.iapi.store.access.conglomerate.SortFactory";

    /**
     * Return an open SortCostController.
     * <p>
     * Return an open SortCostController which can be used to ask about 
     * the estimated costs of SortController() operations.
     * <p>
     *
	 * @return The open StoreCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see  com.splicemachine.db.iapi.store.access.StoreCostController
     **/
    SortCostController openSortCostController()
		throws StandardException;
}
