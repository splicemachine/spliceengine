package com.splicemachine.derby.impl.sql.execute.AlterTable;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;

import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
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
    private boolean isTraced;

    public ConglomerateLoader(long toConglomId,
                              String txnId, boolean isTraced) {

        this.toConglomId = toConglomId;
        this.txnId = txnId;
        this.isTraced = isTraced;
    }

    private void initialize() {
        byte[] table = Bytes.toBytes(Long.toString(toConglomId));
        MetricFactory metricFactory = isTraced? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
        writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(table, txnId, metricFactory);
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

    public WriteStats getWriteStats() {
        return writeBuffer.getWriteStats();
    }
}
