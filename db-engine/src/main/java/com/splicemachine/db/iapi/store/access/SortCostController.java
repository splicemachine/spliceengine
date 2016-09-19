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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.OrderByList;

/**
 * The SortCostController interface provides methods that an access client
 * (most likely the system optimizer) can use to get sorter's estimated cost of
 * various operations on the SortController.
 *
 * @see TransactionController#createSort
 * @see RowCountable
 */

public interface SortCostController{
    /**
     * Close the controller.
     * <p/>
     * Close the open controller.  This method always succeeds, and never
     * throws any exceptions. Callers must not use the StoreCostController
     * after closing it; they are strongly advised to clear
     * out the StoreCostController reference after closing.
     * <p/>
     */
    void close();

    /**
     *
     * Estimate the sort cost.
     *
     * @param baseCost
     * @throws StandardException
     */
    void estimateSortCost(CostEstimate baseCost) throws StandardException;

}
