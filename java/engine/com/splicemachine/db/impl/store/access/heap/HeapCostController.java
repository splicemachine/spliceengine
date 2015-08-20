/*

   Derby - Class com.splicemachine.db.impl.store.access.heap.HeapCostController

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

package com.splicemachine.db.impl.store.access.heap;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.iapi.store.raw.ContainerHandle;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.store.access.conglomerate.GenericCostController;
import com.splicemachine.db.impl.store.access.conglomerate.OpenConglomerate;

import java.util.BitSet;
import java.util.List;
import java.util.Properties;


/**
 * The StoreCostController interface provides methods that an access client
 * (most likely the system optimizer) can use to get store's estimated cost of
 * various operations on the conglomerate the StoreCostController was opened
 * for.
 * <p/>
 * It is likely that the implementation of StoreCostController will open
 * the conglomerate and will leave the conglomerate open until the
 * StoreCostController is closed.  This represents a significant amount of
 * work, so the caller if possible should attempt to open the StoreCostController
 * once per unit of work and rather than close and reopen the controller.  For
 * instance if the optimizer needs to cost 2 different scans against a single
 * conglomerate, it should use one instance of the StoreCostController.
 * <p/>
 * The locking behavior of the implementation of a StoreCostController is
 * undefined, it may or may not get locks on the underlying conglomerate.  It
 * may or may not hold locks until end of transaction.
 * An optimal implementation will not get any locks on the underlying
 * conglomerate, thus allowing concurrent access to the table by a executing
 * query while another query is optimizing.
 * <p/>
 * The StoreCostController gives 2 kinds of cost information
 */

public class HeapCostController
        extends GenericCostController implements StoreCostController{
    /**
     * Only lookup these estimates from raw store once.
     */
    long num_pages;
    long num_rows;
    long page_size;
    long row_size;

    /* Private/Protected methods of This class: */

    /**
     * Initialize the cost controller.
     * <p/>
     * Let super.init() do it's work and then get the initial stats about the
     * table from raw store.
     *
     * @throws StandardException Standard exception policy.
     */
    @Override
    public void init( OpenConglomerate open_conglom) throws StandardException{
        super.init(open_conglom);

        ContainerHandle container=open_conglom.getContainer();

        // look up costs from raw store.
        num_rows=container.getEstimatedRowCount(/*unused flag*/ 0);

        // Don't use 0 rows (use 1 instead), as 0 rows often leads the 
        // optimizer to produce plans which don't use indexes because of the 0 
        // row edge case.
        //
        // Eventually the plan is recompiled when rows are added, but we
        // have seen multiple customer cases of deadlocks and timeouts 
        // because of these 0 row based plans.  
        if(num_rows==0)
            num_rows=1;

        // eliminate the allocation page from the count.
        num_pages=container.getEstimatedPageCount(/* unused flag */ 0);

        Properties prop=new Properties();
        prop.put(Property.PAGE_SIZE_PARAMETER,"");
        container.getContainerProperties(prop);
        page_size=
                Integer.parseInt(prop.getProperty(Property.PAGE_SIZE_PARAMETER));

        row_size=(num_pages*page_size/num_rows);

    }

    @Override
    public long cardinality(int columnNumber) {
        return num_rows;
    }


    @Override
    public long getConglomerateAvgRowWidth() {
        return 20;
    }

    @Override
    public long getBaseTableAvgRowWidth() {
        return 20;
    }

    @Override
    public double getLocalLatency() {
        return 1.0d;
    }

    @Override
    public double getRemoteLatency() {
        return 40.0d;
    }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public double conglomerateColumnSizeFactor(BitSet validColumns) {
        return 1.0d;
    }

    @Override
    public double baseTableColumnSizeFactor(BitSet validColumns) {
        return 1.0d;
    }


}

