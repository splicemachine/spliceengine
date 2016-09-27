/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.ConglomerateFactory;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerateFactory;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.util.Properties;


/**
 *
 *
 **/

public class HBaseConglomerateFactory extends SpliceConglomerateFactory{
    protected static final Logger LOG=Logger.getLogger(HBaseConglomerate.class);

    public HBaseConglomerateFactory(){
        super();
    }

    @Override
    protected String getImplementationID(){
        return "heap";
    }

    @Override
    protected String getFormatUUIDString(){
        return "D2976090-D9F5-11d0-B54D-00A024BF8878";
    }

    @Override
    public int getConglomerateFactoryId(){
        return (ConglomerateFactory.HEAP_FACTORY_ID);
    }

    /**
     * Create the conglomerate and return a conglomerate object for it.
     *
     * @throws StandardException Standard exception policy.
     * @see ConglomerateFactory#createConglomerate
     **/
    public Conglomerate createConglomerate(
            TransactionManager xact_mgr,
            long input_containerid,
            DataValueDescriptor[] template,
            ColumnOrdering[] columnOrder,
            int[] collationIds,
            Properties properties,
            int temporaryFlag)
            throws StandardException{
        HBaseConglomerate hbase=new HBaseConglomerate();

        SIDriver driver=SIDriver.driver();
        hbase.create(
                xact_mgr.getRawStoreXact(),
                input_containerid,
                template,
                columnOrder,
                collationIds,
                properties,
                hbase.getTypeFormatId(),
                temporaryFlag,
                driver.getOperationFactory(),
                driver.getTableFactory());

        return hbase;
    }

    /**
     * Return Conglomerate object for conglomerate with container_key.
     * <p/>
     * Return the Conglomerate Object.  This is implementation specific.
     * Examples of what will be done is using the key to find the file where
     * the conglomerate is located, and then executing implementation specific
     * code to instantiate an object from reading a "special" row from a
     * known location in the file.  In the btree case the btree conglomerate
     * is stored as a column in the control row on the root page.
     * <p/>
     * This operation is costly so it is likely an implementation using this
     * will cache the conglomerate row in memory so that subsequent accesses
     * need not perform this operation.
     *
     * @param xact_mgr      transaction to perform the create in.
     * @param containerId The unique id of the existing conglomerate.
     * @return An instance of the conglomerate.
     * @throws StandardException Standard exception policy.
     **/
    public Conglomerate readConglomerate(TransactionManager xact_mgr,long containerId) throws StandardException{
        SpliceLogUtils.trace(LOG,"readConglomerate container_key %d",containerId);
        return ConglomerateUtils.readConglomerate(containerId,HBaseConglomerate.class,((SpliceTransactionManager)xact_mgr).getActiveStateTxn());
    }
}

