/*

   Derby - Class com.splicemachine.db.impl.store.access.conglomerate.GenericCostController

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

package com.splicemachine.db.impl.store.access.conglomerate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**

 A Generic class which implements the basic functionality needed for a cost
 controller.

 **/

public abstract class GenericCostController extends GenericController implements StoreCostController {

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods implementing StoreCostController class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods implementing StoreCostController class, default impl
     *      just throws exception:
     **************************************************************************
     */


    /**
     * Return the cost of exact key lookup.
     * <p>
     * Return the estimated cost of calling ScanController.fetch()
     * on the current conglomerate, with start and stop positions set such
     * that an exact match is expected.
     * <p>
     * This call returns the cost of a fetchNext() performed on a scan which
     * has been positioned with a start position which specifies exact match
     * on all keys in the row.
     * <p>
     * Example:
     * <p>
     * In the case of a btree this call can be used to determine the cost of
     * doing an exact probe into btree, giving all key columns.  This cost
     * can be used if the client knows it will be doing an exact key probe
     * but does not have the key's at optimize time to use to make a call to
     * getScanCost()
     * <p>
     *
     * @param validColumns    A description of which columns to return from
     *                        row on the page into "templateRow."  templateRow,
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
     *
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ScanController.  
     *
     *                        STORECOST_CLUSTERED - The location of one scan
     *                            is likely clustered "close" to the previous 
     *                            scan.  For instance if the query plan were
     *                            to used repeated "reopenScan()'s" to probe
     *                            for the next key in an index, then this flag
     *                            should be be specified.  If this flag is not 
     *                            set then each scan will be costed independant
     *                            of any other predicted scan access.
     *
     * @return The cost of the fetch.
     *
     * @exception  StandardException  Standard exception policy.
     *
     * @see com.splicemachine.db.iapi.store.access.RowUtil
     **/
    public void getFetchFromFullKeyCost(FormatableBitSet validColumns,
                                        int access_type,
                                        CostEstimate cost) throws StandardException{
        // Not implemented in default conglomerate, needs to be overridden.
        throw StandardException.newException( SQLState.HEAP_UNIMPLEMENTED_FEATURE);
    }

    public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {
        // Not Implemented...
        throw StandardException.newException( SQLState.HEAP_UNIMPLEMENTED_FEATURE);
    }

//    @Override
    public double getSelectivity(int columnNumber,DataValueDescriptor start,boolean includeStart,DataValueDescriptor stop,boolean includeStop){
        return 1.0d;
    }

//    @Override
    public double cardinalityFraction(int columnNumber){
        return 1.0d; //not really implemented
    }

    //    @Override
    public double nullSelectivity(int columnNumber){
        return 1.0d;
    }
}
