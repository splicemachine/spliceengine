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
import java.util.Collections;
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
    public int insert(ExecRow row) throws StandardException{
        return batchInsert(Collections.singletonList(row));
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
    public void insertAndFetchLocation(ExecRow row,
                                       RowLocation destRowLocation) throws StandardException{
        batchInsertAndFetchLocation(new ExecRow[]{row},new RowLocation[]{destRowLocation});
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
