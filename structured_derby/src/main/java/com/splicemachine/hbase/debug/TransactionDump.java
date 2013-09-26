package com.splicemachine.hbase.debug;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TransactionStatus;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
public class TransactionDump extends DebugTask {

    public TransactionDump() {
    }

    public TransactionDump(String jobId, String destinationDirectory) {
        super(jobId, destinationDirectory);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(region.getStartKey());
        scan.setStopRow(region.getEndKey());
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
        scan.setAttribute(SI_EXEMPT, Bytes.toBytes(true));
        scan.setCaching(100);
        scan.setBatch(100);
        //we leave the blocks cached cause we want to keep SI stuff in the cache if possible

        try{
            RegionScanner scanner = null;
            Writer writer = null;
            region.startRegionOperation();
            try{
                scanner = region.getScanner(scan);
                writer = getWriter();

                List<KeyValue> keyValues = Lists.newArrayListWithExpectedSize(12);
                boolean shouldContinue;
                do{
                    keyValues.clear();
                    shouldContinue = scanner.nextRaw(keyValues,null);
                    writeRow(writer,keyValues);
                }while(shouldContinue);

            }finally{
                if(writer!=null){
                    writer.flush();
                    writer.close();
                }
                if(scanner!=null)
                    scanner.close();
                region.closeRegionOperation();
            }
        }catch(Exception e){
            throw new ExecutionException(e);
        }
    }

    private final byte[] TXN_ID_COL = Bytes.toBytes(SIConstants.TRANSACTION_ID_COLUMN);
    private final byte[] GLOBAL_COMMIT_COL = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    private final byte[] BEGIN_TIMESTAMP = Bytes.toBytes(SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN);
    private final byte[] STATUS = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    private final byte[] COMMIT_TIMESTAMP = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    private final byte[] COUNTER = Bytes.toBytes(SIConstants.TRANSACTION_COUNTER_COLUMN);
    private final byte[] PARENT = Bytes.toBytes(SIConstants.TRANSACTION_PARENT_COLUMN);
    private final byte[] WRITES = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    private final byte[] DEPENDENT = Bytes.toBytes(SIConstants.TRANSACTION_DEPENDENT_COLUMN);
    private final byte[] UNCOMMITTED = Bytes.toBytes(SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN);
    private final byte[] COMMITTED = Bytes.toBytes(SIConstants.TRANSACTION_READ_COMMITTED_COLUMN);
    private final byte[] KEEP_ALIVE = Bytes.toBytes(SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN);

    private void writeRow(Writer writer, List<KeyValue> keyValues) throws IOException {
        if(keyValues.size()<=0) return;

        long id = -1;
        Long globalCommit = null;
        long beginTimestamp = Long.MAX_VALUE;
        TransactionStatus txnStatus = null;
        Long commitTimestamp = null;
        Long counter = null;
        Long parent = null;
        Boolean writes = null;
        Boolean dependent = null;
        Boolean readUncommitted = null;
        Boolean readCommitted = null;
        String keepAliveValue = "";

        for(KeyValue kv:keyValues){
            if(!kv.matchingFamily(SpliceConstants.DEFAULT_FAMILY_BYTES))
                continue;
            if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,TXN_ID_COL))
                id = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,GLOBAL_COMMIT_COL))
                globalCommit = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,BEGIN_TIMESTAMP))
                beginTimestamp = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,STATUS))
                txnStatus = TransactionStatus.values()[Bytes.toInt(kv.getValue())];
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,COMMIT_TIMESTAMP))
                commitTimestamp = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,COUNTER))
                counter = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,PARENT))
                parent = Bytes.toLong(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,WRITES))
                writes = Bytes.toBoolean(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,DEPENDENT))
                dependent = Bytes.toBoolean(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,UNCOMMITTED))
                readUncommitted = Bytes.toBoolean(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,COMMITTED))
                readCommitted = Bytes.toBoolean(kv.getValue());
            else if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,KEEP_ALIVE))
                keepAliveValue = Bytes.toString(kv.getValue());
        }
        String lineFormat = "%-8d\t%-8d\t%-8d\t%-12s\t%-8d\t%-8d\t%-8d\t%b\t%b\t%b\t%b\t%s%n";
        String line = String.format(lineFormat,id,globalCommit,beginTimestamp,txnStatus.name(),commitTimestamp,counter,parent,dependent,writes,readUncommitted,readCommitted,keepAliveValue);
        writer.write(line);
    }

    @Override
    protected String getTaskType() {
        return "transactionDump";
    }
}
