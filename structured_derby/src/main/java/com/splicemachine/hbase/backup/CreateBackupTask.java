package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
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
	    public CreateBackupTask() { }

	    public CreateBackupTask(BackupItem backupItem, String jobId) {
	    	super(jobId, OperationJob.operationTaskPriority,backupItem.getBackup().getBackupTransactionIDAsString(),false);
	    	this.backupItem = backupItem;
	    }

	    @Override
	    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
	        super.prepareTask(rce, zooKeeper);
	    }

	    @Override
	    protected String getTaskType() {
	        return "createBackupTask";
	    }

	    @Override
	    public void writeExternal(ObjectOutput out) throws IOException {
	        super.writeExternal(out);
	        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
	    }

	    @Override
	    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	        super.readExternal(in);
	        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
	    }

	    @Override
	    public boolean invalidateOnClose() {
	        return true;
	    }
	    @Override
	    public void doExecute() throws ExecutionException, InterruptedException {
	    	if (LOG.isTraceEnabled())
	    		SpliceLogUtils.trace(LOG, String.format("executing %S backup on region %s",backupItem.getBackup().isIncrementalBackup()?"incremental":"full", region.toString()));	    		
	    	BackupUtils.writeRegionToBackupDirectory(region, backupItem.getBackupItemFilesystem(), region.getFilesystem());
	    		//try {
	    		//	System.out.println("inserting Backup?");
					//backupItem.insertBackupItem(); Not Working Yet
/*	    		} catch (SQLException e) {
	    			System.out.println("inserting Backup Exception?");
	    			e.printStackTrace();
					new ExecutionException(e);
				}
				*/
	    }

	    @Override
	    public int getPriority() {
	            return SchedulerPriorities.INSTANCE.getBasePriority(CreateBackupTask.class);
	    }
	}

