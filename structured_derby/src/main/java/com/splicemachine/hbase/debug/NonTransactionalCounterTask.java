package com.splicemachine.hbase.debug;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
public class NonTransactionalCounterTask extends ZkTask{
    private String destinationDirectory;
    private HRegion region;

    public NonTransactionalCounterTask() { }

    public NonTransactionalCounterTask(String jobId,
                                       int priority,
                                       boolean readOnly,
                                       String destinationDirectory) {
        super(jobId, priority, null, readOnly);
        this.destinationDirectory = destinationDirectory;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(region.getStartKey());
        scan.setStopRow(region.getEndKey());
        scan.setCacheBlocks(false);
        scan.setCaching(100);
        scan.setBatch(100);
        scan.setAttribute(SI_EXEMPT, Bytes.toBytes(true));

        RegionScanner scanner = null;
        long totalCount=0l;
        try{
            scanner = region.getScanner(scan);
            List<KeyValue> keyValues = Lists.newArrayList();
            region.startRegionOperation();
            try{
                boolean shouldContinue;
                do{
                    shouldContinue = scanner.nextRaw(keyValues,null);
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

        FileSystem fs = region.getFilesystem();
        Path outputPath = new Path(destinationDirectory,region.getRegionNameAsString());
        OutputStream os =  null;
        try{
            os = fs.create(outputPath);
            String outputText = String.format("%d%nFINISHED%n",totalCount);
            os.write(outputText.getBytes());
            os.flush();
        } catch (IOException e) {
            throw new ExecutionException("Unable to write output for region "+ region.getRegionNameAsString()+". Answer is "+ totalCount,e);
        } finally{
            Closeables.closeQuietly(os);
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(destinationDirectory);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        destinationDirectory = in.readUTF();
    }
}
