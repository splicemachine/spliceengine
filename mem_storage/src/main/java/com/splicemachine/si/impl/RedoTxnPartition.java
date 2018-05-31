/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.functions.Version3DataScanner;
import com.splicemachine.storage.*;
import com.splicemachine.storage.util.MappedDataResultScanner;
import org.apache.commons.lang.SerializationUtils;

import static com.splicemachine.si.impl.functions.ScannerFunctionUtils.*;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class RedoTxnPartition extends TxnPartition{
    private TxnSupplier txnSupplier;
    private OperationFactory operationFactory;

    public RedoTxnPartition(Partition basePartition,
                            Transactor transactor,
                            RollForward rollForward,
                            TxnOperationFactory txnOpFactory,
                            TransactionReadController txnReadController,
                            ReadResolver readResolver,
                            TxnSupplier txnSupplier,
                            OperationFactory operationFactory
                            ){
        super(basePartition,transactor,rollForward,txnOpFactory,txnReadController,readResolver);
        this.txnSupplier = txnSupplier;
        this.operationFactory = operationFactory;
    }

    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        txnReadController.preProcessGet(get);
        SpliceQuery spliceQuery = operationFactory.getQuery(get);
        TxnView txnView=txnOpFactory.fromReads(get);
        return txnView!=null?
                transformResult(basePartition.get(get,previous),txnView,txnSupplier,basePartition,spliceQuery,operationFactory,txnOpFactory)
                :
                basePartition.get(get,previous);
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        MGet get = new MGet();
        get.setAllAttributes(attributes.allAttributes());
        SpliceQuery spliceQuery = operationFactory.getQuery(get);
        TxnView txnView=txnOpFactory.fromReads(get);
        if (spliceQuery == null && txnView!=null)
            throw new UnsupportedOperationException("get does not have query attached");
        txnReadController.preProcessGet(get);
        List<DataResult> results = new ArrayList<>(rowKeys.size());
        for(byte[] key:rowKeys){
            get.setKey(key);
            DataResult dataResult = basePartition.get(get,null);
            results.add(
                    txnView!=null?
                            transformResult(dataResult,txnView,txnSupplier,basePartition,spliceQuery,operationFactory,txnOpFactory)
                            :
                            basePartition.get(get,null));
        }
        return results.iterator();
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        txnReadController.preProcessScan(scan);
        SpliceQuery spliceQuery = operationFactory.getQuery(scan);
        TxnView txnView = txnOpFactory.fromReads(scan);
        if (spliceQuery == null && txnView!=null)
            throw new UnsupportedOperationException("get does not have query attached");
        return txnView!=null?
                new Version3DataScanner(basePartition.openScanner(scan,metricFactory),16,txnView,txnSupplier,basePartition,
                        spliceQuery,operationFactory,txnOpFactory)
                :
                 basePartition.openScanner(scan,metricFactory);
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        DataScanner ds = openScanner(scan,metricFactory);
        return new MappedDataResultScanner(ds){
            @Override
            protected DataResult newResult(){
                return new MResult();
            }

            @Override
            protected void setResultRow(List<DataCell> nextRow,DataResult resultWrapper){
                ((MResult)resultWrapper).set(nextRow);
            }
        };
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        throw new UnsupportedOperationException("Not Supported");
        /*
         * In SI logic, we never physically delete things, except in the context of explicit physical management
         * (like Compaction in HBase, etc), so we trade this DataDelete in for a DataPut, and add a tombstone
         * instead.
         */
        /*
        TxnView txnView=txnOpFactory.fromWrites(delete);
        if(txnView==null)
            throw new IOException("Direct deletes are not supported under Snapshot Isolation");
        DataPut dp=txnOpFactory.newDataPut(txnView,delete.key());
        for(DataCell dc : delete.cells()){
            dp.tombstone(dc.version());
        }
        dp.setAllAttributes(delete.allAttributes());
        dp.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);

        put(dp);
        */
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof DataPut)
            put((DataPut)put);
        else delete((DataDelete)put);

    }
}
