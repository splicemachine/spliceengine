package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
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
    protected String destinationDirectory;
    protected HRegion region;
		protected byte[] scanStop;
		protected byte[] scanStart;

		public DebugTask() { }

    protected DebugTask(String jobId, String destinationDirectory) {
        super(jobId, 1,null);
        this.destinationDirectory = destinationDirectory;
    }

    @Override
    public void prepareTask(byte[] start, byte[] stop,RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(start,stop,rce, zooKeeper);
				this.scanStart = start;
				this.scanStop = stop;
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
}
