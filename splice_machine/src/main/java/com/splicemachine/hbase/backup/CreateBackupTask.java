package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;
/**
 *
 * \begin{enumerate}
 \item Capture backup timestamp $T_{backup}$ and verify no active backups
 running.
 \item Write Backup Record to Splice Backup Table with current system timestamp
 and flag set to full backup.
 \item Submit CreateFullBackupJob into the backup scheduling tier
 \begin{enumerate}
 \item Obtain region info for all tables in Splice Machine
 \item Submit CreateFullBackupTask to each region in the backup resource queue
 \begin{enumerate}
 \item Perform synchonous region flush
 \item Perform a Start Region Operation (Read Lock)
 \item Validate Begin and End Keys for task are correct for each region, if not
 fail the task for resubmission to new split regions
 \item Capture the set of Data Files for each Region
 \item Copy those files and a meta file capturing the region splits to the
 external filesystem supplied (HDFS, scp, etc.)
 \item Generate checksum
 \item Finish Region Operation
 \item Complete Task
 \end{enumerate}
 \item On Failure, retry.
 \item After all complete, finish the job
 \end{enumerate}
 \item Mark the backup as complete
 \end{enumerate}
 *
 *
 */
public class CreateBackupTask extends ZkTask {
    private static final long serialVersionUID = 5l;
    private BackupItem backupItem;
    private String backupFileSystem;
    public CreateBackupTask() { }

    public CreateBackupTask(BackupItem backupItem, String jobId, String backupFileSystem) {
        super(jobId, OperationJob.operationTaskPriority);
        this.backupItem = backupItem;
        this.backupFileSystem = backupFileSystem;
    }

    @Override
    protected String getTaskType() {
        return "createBackupTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
        out.writeUTF(backupFileSystem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
        backupFileSystem = in.readUTF();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        return new CreateBackupTask(backupItem, jobId, backupFileSystem);
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, 
            		String.format("executing %S backup on region %s",
            				backupItem.getBackup().isIncrementalBackup()?"incremental":"full", region.toString()));
        try {

            if(backupItem.getBackup().isIncrementalBackup()){
            	doIncrementalBackup();
            } else{
            	doFullBackup();
            }
        	writeRegioninfoOnFilesystem();

        } catch (IOException e) {
            throw new ExecutionException(e);
        }
        
        // TODO: insert backup item
    }

    private void writeRegioninfoOnFilesystem() throws IOException
    {
    	DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
        FileSystem fs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);

    	derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(), 
        		new Path(backupItem.getBackupItemFilesystem()+"/"+derbyFactory.getRegionDir(region).getName()+
        				"/"+BackupUtils.REGION_INFO), fs, SpliceConstants.config);
    }
    
    private void doFullBackup() throws IOException
    {
    	SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
    	List<Path> files = utils.getFilesForFullBackup(getSnapshotName(), region);
    	String backupDirectory = backupItem.getBackupItemFilesystem();
    	String name = region.getRegionNameAsString();
    	FileSystem backupFs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);
    	for(Path file: files){
            FileSystem fs = region.getFilesystem();
            // TODO dst path?
	    	FileUtil.copy(fs, file, backupFs,  new Path(backupDirectory+"/"+name), false, SpliceConstants.config);
    	}
    }
    

    
	private void doIncrementalBackup()
    {
    	// TODO
    }
    
    private String getSnapshotName()
    {
    	return backupItem.getBackupItem() + "_"+ backupItem.getBackup().getBackupTimestamp();
    }
    
    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(CreateBackupTask.class);
    }
}

