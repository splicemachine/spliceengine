package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.coprocessor.TxnMessage;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

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

public interface TxnDecoder {

    TxnMessage.Txn decode(SDataLib<OperationWithAttributes,Cell,Delete, Get,
            Put,RegionScanner,Result,Scan> dataLib, long txnId, Result result) throws IOException;

    TxnMessage.Txn decode(SDataLib<OperationWithAttributes, Cell, Delete, Get,
            Put, RegionScanner, Result, Scan> datalib,List<Cell> keyValues) throws IOException;

	Put encodeForPut(TxnMessage.TxnInfo txn) throws IOException;

}
