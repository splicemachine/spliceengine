package com.splicemachine.si.api.txn;

import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.coprocessor.TxnMessage;

import java.io.IOException;
import java.util.List;

/**
 * Interface for different mechanisms for encoding/decoding
 * transactions from the transaction table storage.
 *
 * This is an interface so that we can support both the
 * old (non-packed) table format and the new format
 * simultaneously.
 *
 * @author Scott Fines
 * Date: 8/14/14
 *
 */

public interface TxnDecoder<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Put extends OperationWithAttributes,RegionScanner,Result,
        Scan extends OperationWithAttributes,OperationStatus> {

    TxnMessage.Txn decode(SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib, long txnId, Result result) throws IOException;

    TxnMessage.Txn decode(SDataLib<OperationWithAttributes, Data, Delete, Filter, Get,
            Put, RegionScanner, Result, Scan> datalib,List<Data> keyValues) throws IOException;

	Put encodeForPut(TxnMessage.TxnInfo txn) throws IOException;
        
    TxnMessage.Txn composeValue(Data destinationTables,IsolationLevel level,long txnId,long beginTs,long parentTs,boolean hasAdditive,
                                boolean additive,long commitTs,long globalCommitTs,Txn.State state,long kaTime);
    
}
