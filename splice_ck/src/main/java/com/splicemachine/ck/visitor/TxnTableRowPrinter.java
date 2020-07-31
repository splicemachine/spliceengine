package com.splicemachine.ck.visitor;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.region.V2TxnDecoder;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TxnTableRowPrinter implements IRowPrinter {

    @Override
    public List<String> ProcessRow(Result row) throws Exception {
        V2TxnDecoder decoder = V2TxnDecoder.INSTANCE;
        TxnMessage.Txn txn = decoder.decode(null, row.listCells());
        List<String> result = new ArrayList<>();
        result.add(Long.toString(txn.getInfo().getTxnId()));
        result.add(Long.toString(txn.getCommitTs()));
        result.add(Long.toString(txn.getGlobalCommitTs()));
        result.add(Long.toString(txn.getInfo().getParentTxnid()));
        result.add(Txn.State.fromInt(txn.getState()).toString());
        result.add(Txn.IsolationLevel.fromInt(txn.getInfo().getIsolationLevel()).toHumanFriendlyString());
        result.add(Boolean.toString(txn.getInfo().getIsAdditive()));
        result.add(Long.toString(txn.getLastKeepAliveTime()));
        result.add(txn.getRollbackSubIdsList().toString());
        final Iterator<ByteSlice> destinationTablesIterator = V2TxnDecoder.decodeDestinationTables(txn.getInfo().getDestinationTables());
        List<String> tables = new ArrayList<>();
        while(destinationTablesIterator.hasNext()) {
            tables.add(Bytes.toString(destinationTablesIterator.next().array()));
        }
        result.add(tables.toString());
        return result;
    }
}
