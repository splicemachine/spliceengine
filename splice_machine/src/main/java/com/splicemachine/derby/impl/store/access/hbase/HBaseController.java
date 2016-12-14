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

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.Partition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HBaseController extends SpliceController{
    protected static final Logger LOG=Logger.getLogger(HBaseController.class);

    public HBaseController(OpenSpliceConglomerate openSpliceConglomerate,
                           Transaction trans,
                           PartitionFactory partitionFactory,
                           TxnOperationFactory txnOperationFactory){
        super(openSpliceConglomerate,trans,partitionFactory,txnOperationFactory);
    }

    @Override
    public int batchInsert(List<ExecRow> rows) throws StandardException {
        int i = 0;
        List<DataPut> puts = new ArrayList<>();
        for (ExecRow row: rows) {
            assert row != null : "Cannot insert a null row!";
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("batchInsert into conglom %d row %s with txnId %s",
                        openSpliceConglomerate.getConglomerate().getContainerid(), (Arrays.toString(row.getRowArray())), trans.getTxnInformation()));
            try {
                DataPut put = opFactory.newDataPut(trans.getTxnInformation(), SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
                encodeRow(row.getRowArray(), put, null, null);
                puts.add(put);
                i++;
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }
        try {
            Partition htable = getTable();
            htable.writeBatch(puts.toArray(new DataPut[puts.size()]));
            return 0;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int insert(DataValueDescriptor[] row) throws StandardException{
        assert row!=null: "Cannot insert a null row!";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("insert into conglom %d row %s with txnId %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),(Arrays.toString(row)),trans.getTxnInformation()));
        Partition htable=getTable();
        try{
            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
            encodeRow(row,put,null,null);
            htable.put(put);
            return 0;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }


    @Override
    public void batchInsertAndFetchLocation(ExecRow[] rows, RowLocation[] rowLocations) throws StandardException {
        List<DataPut> puts = new ArrayList();
       int i = 0;
        for (ExecRow row: rows) {
            assert row != null : "Cannot insert into a null row!";
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("insertAndFetchLocation --> into conglom %d row %s",
                        openSpliceConglomerate.getConglomerate().getContainerid(), Arrays.toString(row.getRowArray())));
            try {
                DataPut put = opFactory.newDataPut(trans.getTxnInformation(), SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
                encodeRow(row.getRowArray(), put, null, null);
                rowLocations[i].setValue(put.key());
                puts.add(put);
                i++;
            } catch (Exception e) {
                throw StandardException.newException("insert and fetch location error", e);
            }
        }
        Partition htable = getTable(); //-sf- don't want to close the htable here, it might break stuff
        try {
            htable.writeBatch(puts.toArray(new DataPut[puts.size()]));
        } catch (Exception e) {
            throw StandardException.newException("insert and fetch location error", e);
        }
    }

    @Override
    public void insertAndFetchLocation(DataValueDescriptor[] row,
                                       RowLocation destRowLocation) throws StandardException{
        assert row!=null: "Cannot insert into a null row!";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("insertAndFetchLocation into conglom %d row %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),Arrays.toString(row)));

        Partition htable=getTable(); //-sf- don't want to close the htable here, it might break stuff
        try{
            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
            encodeRow(row,put,null,null);
            destRowLocation.setValue(put.key());
            htable.put(put);
        }catch(Exception e){
            throw StandardException.newException("insert and fetch location error",e);
        }
    }



    @Override
    public boolean replace(RowLocation loc,DataValueDescriptor[] row,FormatableBitSet validColumns) throws StandardException{
        assert row!=null: "Cannot replace with a null row";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("replace rowLocation %s, destRow %s, validColumns %s",loc,Arrays.toString(row),validColumns));
        Partition htable=getTable();
        try{

            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),loc.getBytes());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());

            encodeRow(row,put,EngineUtils.bitSetToMap(validColumns),validColumns);
            htable.put(put);
            return true;
        }catch(Exception e){
            throw StandardException.newException("Error during replace ",e);
        }
    }
}
