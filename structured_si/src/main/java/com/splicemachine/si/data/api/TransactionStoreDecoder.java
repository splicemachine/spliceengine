package com.splicemachine.si.data.api;

import com.splicemachine.si.impl.Transaction;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * Decoder for transaction table entries. This is written as an interface
 * because there is a possibility that multiple decoders may be used.
 *
 * @author Scott Fines
 * Date: 7/30/14
 */
public interface TransactionStoreDecoder {

    void initialize() throws IOException;

    Transaction decode(List<KeyValue> keyValues) throws IOException;

}
