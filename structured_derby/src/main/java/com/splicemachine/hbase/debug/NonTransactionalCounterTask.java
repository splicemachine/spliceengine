package com.splicemachine.hbase.debug;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class NonTransactionalCounterTask extends DebugTask{

    public NonTransactionalCounterTask() { }

    public NonTransactionalCounterTask(String jobId,
                                       int priority,
                                       boolean readOnly,
                                       String destinationDirectory) {
        super(jobId, destinationDirectory);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(region.getStartKey());
        scan.setStopRow(region.getEndKey());
        scan.setCacheBlocks(false);
        scan.setCaching(100);
        scan.setBatch(100);
        scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);

        RegionScanner scanner = null;
        long totalCount=0l;
        try{
            scanner = region.getScanner(scan);
            List<Cell> keyValues = Lists.newArrayList();
            region.startRegionOperation();
            try{
                boolean shouldContinue;
                do{
                    keyValues.clear();
                    shouldContinue = scanner.nextRaw(keyValues);
                    if(keyValues.size()>0)
                        totalCount++;
                }while(shouldContinue);
            }finally{
                region.closeRegionOperation();
            }
        } catch (IOException e) {
            throw new ExecutionException(e);
        }finally{
            Closeables.closeQuietly(scanner);
        }

        Writer writer = null;
        try{
            writer = getWriter();
            String outputText = String.format("%d%nFINISHED%n",totalCount);
            writer.write(outputText);
            writer.flush();
        } catch (IOException e) {
            throw new ExecutionException("Unable to write output for region "+ region.getRegionNameAsString()+". Answer is "+ totalCount,e);
        } finally{
            Closeables.closeQuietly(writer);
        }
    }

    @Override
    protected String getTaskType() {
        return "nonTransactionalCounter";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
}
