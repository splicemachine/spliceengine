package com.splicemachine.derby.impl.sql.execute.AlterTable;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;

/**
 *
 * User: jyuan
 * Date: 2/6/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class ConglomerateLoader {

    private static Logger LOG = Logger.getLogger(ConglomerateLoader.class);
    private long toConglomId;
    private String txnId;

    private BufferedRegionScanner brs;
    private RecordingCallBuffer<KVPair> writeBuffer;
    private boolean initialized = false;

    public ConglomerateLoader(long toConglomId,
                              String txnId) {

        this.toConglomId = toConglomId;
        this.txnId = txnId;
    }

    private void initialize() {
        byte[] table = Bytes.toBytes(Long.toString(toConglomId));
        writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(table, txnId);
        initialized = true;
    }

    public void add(KVPair kv) throws Exception{
        if (!initialized) {
            initialize();
        }
        try{
            writeBuffer.add(kv);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw e;
        }
    }

    public void flush() throws Exception{
        try {
            writeBuffer.flushBuffer();
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw e;
        }
    }

    public void close() throws ExecutionException{
        try {
            if (initialized) {
                writeBuffer.close();
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    public long getTotalBytesAdded() {
        return writeBuffer.getTotalBytesAdded();
    }
}
