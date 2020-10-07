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
 * Factory class for the physical creation of storage.
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
     *
     *
     * Create the conglomerate and return a conglomerate object for it.
     *
     * @param isExternal           Whether this table is managed internally or via an external mechanism
     * @param xact_mgr             transaction to perform the create in.
     * @param input_containerid    containerid to assign the container, or
    ContainerHandle.DEFAULT_ASSIGN_ID if you want
     * @param template             Template of row in the conglomerate.
     * @param columnOrder          columns sort order for Index creation
     * @param collationIds         collation ids of columns in the conglomerate.
     * @param properties           Properties associated with the conglomerate.
     * @param temporaryFlag
     * @return
     * @throws StandardException
     *
     * @throws StandardException Standard exception policy.
     * @see ConglomerateFactory#createConglomerate
     *
     *
     */
    @Override
    public Conglomerate createConglomerate(
            boolean isExternal,
            TransactionManager xact_mgr,
            long input_containerid,
            DataValueDescriptor[] template,
            ColumnOrdering[] columnOrder,
            int[] collationIds,
            Properties properties,
            int temporaryFlag,
            byte[][] splitKeys)
            throws StandardException{
        HBaseConglomerate hbase=new HBaseConglomerate();

        SIDriver driver=SIDriver.driver();
        hbase.create(isExternal,
                xact_mgr.getRawStoreXact(),
                input_containerid,
                template,
                columnOrder,
                collationIds,
                properties,
                hbase.getTypeFormatId(),
                temporaryFlag,
                driver.getOperationFactory(),
                driver.getTableFactory(),
                splitKeys);

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

