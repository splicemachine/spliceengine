package com.splicemachine.hbase.debug;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;

/**
 * @author Scott Fines
 *         Created on: 9/24/13
 */
public class ColumnDumpTask extends DebugTask{
    private EntryDecoder decoder = new EntryDecoder();

    private int columnNumber;

    public ColumnDumpTask() { }

    public ColumnDumpTask(String jobId, String destinationDirectory,int columnNumber) {
        super(jobId, destinationDirectory);
        this.columnNumber = columnNumber;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.columnNumber = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(columnNumber);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(region.getStartKey());
        scan.setStopRow(region.getEndKey());
        scan.setCacheBlocks(false);
        scan.setCaching(100);
        scan.setBatch(100);
        scan.setAttribute(SpliceConstants.SI_EXEMPT, Bytes.toBytes(true));

        Writer writer;
        RegionScanner scanner;
        try{

            writer = getWriter();
            scanner = region.getScanner(scan);
            List<Cell> keyValues = Lists.newArrayList();
            region.startRegionOperation();
            System.out.println("Starting scan task");
            try{
                writer.write(String.format("%d%n",System.currentTimeMillis()));
                boolean shouldContinue;
                do{
                    keyValues.clear();
                    shouldContinue = scanner.nextRaw(keyValues);
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

    @Override
    protected String getTaskType() {
        return "debugColumnDump";
    }

    private static final String outputPattern = "%-20s\t%8d\t%s%n";
    private void writeRow(Writer writer,List<Cell> keyValues) throws IOException {
        for(Cell kv:keyValues){
						if(!CellUtils.matchingColumn(kv,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES))
								continue;
            long txnId = kv.getTimestamp();

            byte[] value = kv.getValueArray();
            //split by separator
            decoder.set(value);
            BitIndex encodedIndex = decoder.getCurrentIndex();
            MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
            //loop until the columnNumber is reached
            int pos;
            for(pos=encodedIndex.nextSetBit(0);
                    pos>=0&&pos<columnNumber;pos=encodedIndex.nextSetBit(pos+1)){
                decoder.seekForward(fieldDecoder,pos);
            }
            if(pos>columnNumber)
                continue; //skip if there are no entries
            ByteBuffer buffer = decoder.nextAsBuffer(fieldDecoder, pos);
            byte[] bufferBytes = new byte[buffer.remaining()];
            buffer.get(bufferBytes);
            String data = String.format(outputPattern, BytesUtil.toHex(kv.getRowArray()),txnId,BytesUtil.toHex(bufferBytes));

            writer.write(data);
        }
    }

		@Override
		public int getPriority() {
				return SchedulerPriorities.INSTANCE.getMaxPriority();
		}
}
