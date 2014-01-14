package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 9/17/13
 */
public abstract class DebugTask extends ZkTask {
    private String destinationDirectory;
    protected HRegion region;

    public DebugTask() { }

    protected DebugTask(String jobId, String destinationDirectory) {
        super(jobId, 1,null,true);
        this.destinationDirectory = destinationDirectory;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.destinationDirectory = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(destinationDirectory);
    }

    protected Writer getWriter() throws IOException{
        Path outputPath = new Path(destinationDirectory,region.getRegionInfo().getEncodedName());
        FileSystem fs = region.getFilesystem();
        if(fs.exists(outputPath))
            fs.delete(outputPath,false);

        return new OutputStreamWriter(fs.create(outputPath));
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

		@Override
		public int getPriority() {
				return SchedulerPriorities.INSTANCE.getMaxPriority();
		}

		@Override
    public boolean isTransactional() {
        return false;
    }
}
