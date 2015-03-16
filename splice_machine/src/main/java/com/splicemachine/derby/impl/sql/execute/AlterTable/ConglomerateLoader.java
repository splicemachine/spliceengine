package com.splicemachine.derby.impl.sql.execute.altertable;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.util.concurrent.ExecutionException;

/**
 *
 * User: jyuan
 * Date: 2/6/14
 */
public class ConglomerateLoader {

    private static Logger LOG = Logger.getLogger(ConglomerateLoader.class);
    private long toConglomId;
    private TxnView txn;

    private RecordingCallBuffer<KVPair> writeBuffer;
    private boolean initialized = false;
    private boolean isTraced;

    public ConglomerateLoader(long toConglomId,
                              TxnView txn,boolean isTraced) {

        this.toConglomId = toConglomId;
        this.txn = txn;
        this.isTraced = isTraced;
    }

    private void initialize() {
        byte[] table = Bytes.toBytes(Long.toString(toConglomId));
        MetricFactory metricFactory = isTraced? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
        writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(table, txn, metricFactory);
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
        if(writeBuffer==null) return WriteStats.NOOP_WRITE_STATS;
        return writeBuffer.getWriteStats();
    }
}
