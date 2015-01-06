package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceIndexObserver extends AbstractSpliceIndexObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "prePut %s", put);
        //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
        if (conglomId > 0) {
            if (put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null) {
                return;
            }
            byte[] rowKey = put.getRow();
            List<KeyValue> data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);
            KVPair kv;
            if (data != null && data.size() > 0) {
                byte[] value = data.get(0).getValue();
                if (put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null) {
                    kv = new KVPair(rowKey, value, KVPair.Type.UPDATE);
                } else
                    kv = new KVPair(rowKey, value);
            } else {
                kv = new KVPair(rowKey, HConstants.EMPTY_BYTE_ARRAY);
            }
            mutate(e.getEnvironment(), kv, operationFactory.fromWrites(put));
        }
        super.prePut(e, put, edit, writeToWAL);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "preDelete %s", delete);
        if (conglomId > 0) {
            if (delete.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME) == null) {
                KVPair deletePair = KVPair.delete(delete.getRow());
                TxnView txn = operationFactory.fromWrites(delete);
                mutate(e.getEnvironment(), deletePair, txn);
            }
        }
        super.preDelete(e, delete, edit, writeToWAL);
    }
}
