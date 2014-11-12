package com.splicemachine.hbase.debug;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.tools.LongHashMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class TransactionCountTask extends DebugTask{

    public TransactionCountTask() { }

    public TransactionCountTask(String jobId, String destinationDirectory) {
        super(jobId, destinationDirectory);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(scanStart);
        scan.setStopRow(scanStop);
        scan.setCacheBlocks(false);
        scan.setCaching(100);
        scan.setBatch(100);
        scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES); //just scan SI instead of the data itself

        LongHashMap<Long> txnHashMap = LongHashMap.evictingMap(1000);
        SimpleMeasuredRegionScanner scanner;
        try{

            Writer writer = getWriter();
            scanner = new SimpleMeasuredRegionScanner(region.getScanner(scan),Metrics.noOpMetricFactory());
            region.startRegionOperation();
            try{
                writer.write(String.format("%d%n",System.currentTimeMillis()));
                List keyValues = Lists.newArrayList();
                boolean shouldContinue;
                do{
                    keyValues.clear();
                    shouldContinue = scanner.internalNextRaw(keyValues);
                    if(keyValues.size()>0){
                        putRow(keyValues, txnHashMap, writer);
                    }
                }while(shouldContinue);

                List<LongHashMap.LongEntry<Long>> finalValues = txnHashMap.clear();
                for(LongHashMap.LongEntry<Long> entry:finalValues){
                    if(entry!=null)
                        writeRow(writer,entry);
                }
                writer.write(String.format("FINISHED%n"));
            }finally{
                scanner.close();
                writer.flush();
                writer.close();
                region.closeRegionOperation();
            }
        }catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private static final Comparator<KeyValue> kvComparator = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue o1, KeyValue o2) {
            if(o1==null){
                if(o2==null) return 0;
                return -1;
            }else if(o2==null)
                return 1;
            else{
                byte[] buffer1 = o1.getBuffer();
                int offset1 = o1.getTimestampOffset();

                byte[] b2 = o2.getBuffer();
                int off2 = o2.getTimestampOffset();

                return Bytes.compareTo(buffer1,offset1,Bytes.SIZEOF_LONG,b2,off2,Bytes.SIZEOF_LONG);
            }
        }
    };

    private void putRow(List<KeyValue> keyValues,LongHashMap<Long> counterMap,Writer writer) throws IOException {
        Collections.sort(keyValues,kvComparator);

        byte[] oldBuffer = null;
        int oldOffset = 0;
        int count =0;
        for(KeyValue kv:keyValues){
            if(oldBuffer==null){
                oldBuffer = kv.getBuffer();
                oldOffset = kv.getTimestampOffset();
                count = 1;
                continue;
            }
            byte[] newBuff = kv.getBuffer();
            int newOff = kv.getTimestampOffset();
            if(Bytes.equals(oldBuffer, oldOffset, Bytes.SIZEOF_LONG, newBuff, newOff, Bytes.SIZEOF_LONG)){
                count++;
            }else{
                long oldTs = Bytes.toLong(oldBuffer, oldOffset);
                long currentCount = counterMap.get(oldTs);
                LongHashMap.LongEntry<Long> evicted = counterMap.put(oldTs, currentCount + count);
                if(evicted!=null)
                    writeRow(writer,evicted);

                oldBuffer = newBuff;
                oldOffset = newOff;
                count=1;
            }
        }
        if(oldBuffer!=null){
            long oldTs = Bytes.toLong(oldBuffer, oldOffset);
            Long currentCount = counterMap.get(oldTs);
            if(currentCount==null)
                currentCount = 0l;
            LongHashMap.LongEntry<Long> evicted = counterMap.put(oldTs, currentCount + count);
            if(evicted!=null)
                writeRow(writer,evicted);
        }
    }

    private static final String outputPattern = "%-8d\t%d%n";
    private void writeRow(Writer writer, LongHashMap.LongEntry<Long> evicted) throws IOException {
        writer.write(String.format(outputPattern,evicted.getKey(),evicted.getValue()));
    }

    @Override
    protected String getTaskType() {
        return "transactionCountTask";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

		@Override
		public RegionTask getClone() {
				return new TransactionCountTask(jobId,destinationDirectory);
		}

		@Override
		public boolean isSplittable() {
				return true;
		}
}
