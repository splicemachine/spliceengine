package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.DenseTxn;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

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
 */
interface TxnDecoder {

    Txn decode(long txnId, Result result) throws IOException;

    DenseTxn decode(List<KeyValue> keyValues) throws IOException;
}
