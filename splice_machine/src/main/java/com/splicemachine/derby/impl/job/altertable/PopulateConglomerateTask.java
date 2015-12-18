package com.splicemachine.derby.impl.job.altertable;

import java.util.concurrent.ExecutionException;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;

/**
 * @author Jeff Cunningham
 *         Date: 3/19/15
 */
public class PopulateConglomerateTask{
    private static Logger LOG = Logger.getLogger(PopulateConglomerateTask.class);

    private static final long serialVersionUID = 1L;

    private int expectedScanReadWidth;
    private long demarcationTimestamp;
    private DDLChange ddlChange;

    private RecordingCallBuffer<KVPair> writeBuffer;
    private RowTransformer transformer;
    private SITableScanner scanner;

    private byte[] scanStart;
    private byte[] scanStop;
    private static SDataLib dataLib = SIDriver.driver().getDataLib();
    private KeyEncoder noPKKeyEncoder;

    public PopulateConglomerateTask() { }

    public PopulateConglomerateTask(String jobId,
                                    int expectedScanReadWidth,
                                    long demarcationTimestamp,
                                    DDLChange ddlChange) {
        this.expectedScanReadWidth = expectedScanReadWidth;
        this.demarcationTimestamp = demarcationTimestamp;
        this.ddlChange = ddlChange;
    }

    public void doExecute() throws ExecutionException {
        /*
        try {
            try (SITableScanner scanner = getTableScanner(Metrics.noOpMetricFactory())) {
                ExecRow nextRow = scanner.next();
                while (nextRow != null) {
                    getWriteBuffer().add(getTransformer(getKeyEncoder(scanner)).transform(nextRow));
                    nextRow = scanner.next();
                }
            } finally {
                if (writeBuffer != null) {
                    writeBuffer.flushBuffer();
                    writeBuffer.close();
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
        */
    }
/*
    private SITableScanner getTableScanner(MetricFactory metricFactory) throws IOException {
        if (scanner == null) {
            Txn txn = getTxn();
            Scan scan = SpliceUtils.createScan(txn);
            scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
            scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
            scan.setStartRow(scanStart);
            scan.setStopRow(scanStop);
            scan.setCacheBlocks(false);
            scan.setMaxVersions();

            RegionScanner regionScanner = region.getScanner(scan);

            MeasuredRegionScanner mrs = new BufferedRegionScanner(region,
                                            regionScanner,
                                            scan,
                                            SpliceConstants.DEFAULT_CACHE_SIZE,
                                            SpliceConstants.DEFAULT_CACHE_SIZE,
                                            metricFactory,
                                            dataLib);

            TxnView txnView = new DDLTxnView(txn, this.demarcationTimestamp);

            TableScannerBuilder builder = new TableScannerBuilder();
            builder.region(TransactionalRegions.get(region)).scan(scan).scanner(mrs).transaction(txnView);

            TransformingDDLDescriptor tdl = (TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc();

            this.scanner = tdl.setScannerBuilderProperties(builder).build();
        }
        return scanner;

    }

    private RecordingCallBuffer<KVPair> getWriteBuffer() {
        if (writeBuffer == null) {
            byte[] newTableLocation = Bytes.toBytes(Long.toString(ddlChange.getTentativeDDLDesc().getConglomerateNumber()));
            writeBuffer = SpliceDriver.driver().getTableWriter().noIndexWriteBuffer(newTableLocation, getTxn(),
                                                                                    Metrics.noOpMetricFactory());
        }
        return writeBuffer;
    }

    private RowTransformer getTransformer(KeyEncoder keyEncoder) throws IOException {
        if (transformer == null) {
            transformer = ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createRowTransformer(keyEncoder);
        }
        return transformer;
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        return tc.beginChildTransaction(parentTxn, Long.toString(ddlChange.getTentativeDDLDesc()
                                                                          .getConglomerateNumber()).getBytes());
    }


    public KeyEncoder getKeyEncoder(final SITableScanner scanner) throws StandardException {
        if (noPKKeyEncoder == null) {

            DataHash hash = new DataHash<ExecRow>() {
                @Override
                public void setRow(ExecRow rowToEncode) {
                    // no op
                }

                @Override
                public byte[] encode() throws StandardException, IOException {
                    //Slice performs byte[] copy
                    return scanner.getCurrentRowLocation().getBytes();
                }

                @Override
                public KeyHashDecoder getDecoder() {
                    return NoOpKeyHashDecoder.INSTANCE;
                }

                @Override
                public void close() throws IOException {
                    // No Op
                }
            };
            noPKKeyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, hash, NoOpPostfix.INSTANCE);
        }
        return noPKKeyEncoder;
    }
    */
}
