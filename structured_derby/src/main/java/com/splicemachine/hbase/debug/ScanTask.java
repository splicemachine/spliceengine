package com.splicemachine.hbase.debug;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceZooKeeperManager;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class ScanTask extends DebugTask{
    private EntryPredicateFilter predicateFilter;
    private EntryDecoder decoder = new EntryDecoder();
    private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
    public ScanTask() {
    }

    public ScanTask(String jobId,
                    EntryPredicateFilter predicateFilter,
                    String destinationDirectory) {
        super(jobId, destinationDirectory);
        this.predicateFilter = predicateFilter;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        byte[] data = new byte[in.readInt()];
        in.readFully(data);
        this.predicateFilter = EntryPredicateFilter.fromBytes(data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        byte[] epfBytes = predicateFilter.toBytes();
        out.writeInt(epfBytes.length);
        out.write(epfBytes);
    }


    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(scanStart);
        scan.setStopRow(scanStop);
        scan.setCacheBlocks(false);
        scan.setCaching(100);
        scan.setBatch(100);
        scan.setFilter(new AbstractHBaseEntryPredicateFilter(predicateFilter));
        scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));

        Writer writer;
        SimpleMeasuredRegionScanner scanner;
        try{

            writer = getWriter();
            scanner = new SimpleMeasuredRegionScanner(region.getScanner(scan),Metrics.noOpMetricFactory());
            List keyValues = Lists.newArrayList();
            region.startRegionOperation();
            System.out.println("Starting scan task");
            try{
                writer.write(String.format("%d%n",System.currentTimeMillis()));
                boolean shouldContinue;
                do{
                    keyValues.clear();
                    shouldContinue = scanner.internalNextRaw(keyValues);
                    if(keyValues.size()>0){
                        writeRow(writer,keyValues);
                    }
                }while(shouldContinue);
                //make sure everyone knows we succeeded
                writer.write(String.format("FINISHED%n"));
                System.out.println("Scan Task finished successfully");
            }finally{
                writer.flush();
                writer.close();
                scanner.close();
                region.closeRegionOperation();
            }
        }catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private static final String outputPattern = "%-20s\t%8d\t%s%n";
    private void writeRow(Writer writer,List keyValues) throws IOException {
        for(Object kv:keyValues){
            if(!dataLib.singleMatchingColumn(kv,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES))
                continue;
            String row = BytesUtil.toHex(dataLib.getDataRow(kv));
            long txnId = dataLib.getTimestamp(kv);

            byte[] value = dataLib.getDataValue(kv);
            //split by separator
            decoder.set(value);
            StringBuilder valueStr = new StringBuilder();
            BitIndex encodedIndex = decoder.getCurrentIndex();
            MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
            boolean isFirst=true;
            for(int pos=encodedIndex.nextSetBit(0);
                pos >=0;pos=encodedIndex.nextSetBit(pos+1)){
                if(!isFirst)
                    valueStr = valueStr.append(",");
                else
                    isFirst = false;

                valueStr.append(BytesUtil.toHex(decoder.nextAsBuffer(fieldDecoder, pos)));
            }
            valueStr.append("\n");
            String data = valueStr.toString();

            String line = String.format(outputPattern,row,txnId,data);
            writer.write(line);
        }
    }

    @Override
    protected String getTaskType() {
        return "nonTransactionalScan";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

		@Override
		public RegionTask getClone() {
				return new ScanTask(jobId,predicateFilter,destinationDirectory);
		}

		@Override public boolean isSplittable() { return true; }

		
}
