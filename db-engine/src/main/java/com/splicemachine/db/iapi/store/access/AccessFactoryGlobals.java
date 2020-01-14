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

package com.splicemachine.db.iapi.store.access;

/**

Global constants provided by the Access Interface.

**/

public interface AccessFactoryGlobals
{
    /**************************************************************************
     * Static constants.
     **************************************************************************
     */
    /**
     * The name for user transactions. This name will be displayed by the
     * transactiontable VTI.
     */
	String USER_TRANS_NAME = "UserTransaction";

    /**
     * The name for system transactions. This name will be displayed by the
     * transactiontable VTI.
     */
	String SYS_TRANS_NAME = "SystemTransaction";

	/**
	 *	Overflow Threshold
	 *
	 *  This defined how large the row can be before it becomes a long row,
	 *  during an insert.
	 *
	 *  @see com.splicemachine.db.iapi.store.raw.Page
	 */
	int BTREE_OVERFLOW_THRESHOLD = 50;
	int HEAP_OVERFLOW_THRESHOLD  = 100;
	int SORT_OVERFLOW_THRESHOLD  = 100;

    String CFG_CONGLOMDIR_CACHE = "ConglomerateDirectoryCache";

    String HEAP = "heap";

	String DEFAULT_PROPERTY_NAME = "derby.defaultPropertyName";

	String PAGE_RESERVED_SPACE_PROP = "0";

	String CONGLOM_PROP = "derby.access.Conglomerate.type";

	String IMPL_TYPE = "implType";

	String SORT_EXTERNAL = "sort external";
	String SORT_INTERNAL = "sort internal";
    String SORT_UNIQUEWITHDUPLICATENULLS_EXTERNAL
                                    = "sort almost unique external";

	String NESTED_READONLY_USER_TRANS = "nestedReadOnlyUserTransaction";
	String NESTED_UPDATE_USER_TRANS = "nestedUpdateUserTransaction";

    String RAMXACT_CONTEXT_ID = "RAMTransactionContext";

    String RAMXACT_CHILD_CONTEXT_ID = "RAMChildContext";

    String RAMXACT_INTERNAL_CONTEXT_ID = "RAMInternalContext";

}

