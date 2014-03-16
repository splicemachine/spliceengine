package com.splicemachine.derby.impl.job.AlterTable;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
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
        writeBuffer.add(kv);
    }

    public void flush() {
        try {
            writeBuffer.flushBuffer();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void close() {
        try {
            writeBuffer.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public long getTotalBytesAdded() {
        return writeBuffer.getTotalBytesAdded();
    }
}
