package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.io.IOUtils;
import org.apache.log4j.Logger;

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
public class CreateBackupTask {
	    
	private static final long serialVersionUID = 5l;
    private BackupItem backupItem;
    private String backupFileSystem;
    private static Logger LOG = Logger.getLogger(CreateBackupTask.class);
    
    public CreateBackupTask() { }

    public CreateBackupTask(BackupItem backupItem, String jobId, String backupFileSystem) {
        this.backupItem = backupItem;
        this.backupFileSystem = backupFileSystem;
    }

    public void doExecute() throws ExecutionException, InterruptedException {
        /*
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing %S backup on region %s",backupItem.getBackup().isIncrementalBackup()?"incremental":"full", "TODO"));
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);
            writeRegionInfoOnFilesystem();
            doFullBackup();
        } catch (IOException e) {
            try {
                Path p = new Path(backupFileSystem + "/" + region.getRegionInfo().getEncodedName());
                if (fs != null && fs.exists(p)) {
                    fs.delete(p, true);
                }
            }
            catch (IOException ex) {
                throw new ExecutionException(ex);
            }
            throw new ExecutionException(e);
        }
        */
    }

	private void writeRegionInfoOnFilesystem() throws IOException
    {
        /*
        FileSystem fs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);

        BackupUtils.derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(),
                new Path(backupFileSystem), fs, SpliceConstants.config);
                */
    }
    
    private Configuration getConfiguration()
    {
    	return SpliceConstants.config;
    }
    
	
	private void doFullBackup() throws IOException
    {
        /*
    	SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
    	boolean throttleEnabled = 
    			getConfiguration().getBoolean(Backup.CONF_IOTHROTTLE, false);
    	    	
    	// Backup directory structure is
    	// backupId/namespace/table/region/cf
    	// files can be HFile links or real paths to a 
    	// materialized references
    	List<Object> files = utils.getFilesForFullBackup(getSnapshotName(), region);
    	FileSystem backupFs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);
    	for(Object file: files){
            FileSystem fs = region.getFilesystem();
            String[] s = file.toString().split("/");
            int n = s.length;
            String fileName = s[n - 1];
            String familyName = s[n - 2];
            String regionName = s[n - 3];
            Path destPath = new Path(backupFileSystem + "/" + regionName + "/V/" + fileName);
            if(throttleEnabled){
            	IOUtils.copyFileWithThrottling(fs, file, backupFs,  destPath, false, getConfiguration());
            } else{
            	copyFile(fs, file, backupFs,  destPath, false, SpliceConstants.config);
            }
	    	if(BackupUtils.isTempFile(file)){
                BackupUtils.deleteFile(fs, file, false);
                SpliceLogUtils.trace(LOG, "delete temp file %s", file);
	    	}
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG, "copied %s to %s", file.toString(), destPath.toString());
            }
    	}
    	*/
    }
    


	private void copyFile(FileSystem srcFS, Object file, FileSystem dstFS,
			Path dst, boolean deleteSource, Configuration conf) throws IOException 
	{
		if(file instanceof Path){
			Path src = (Path) file;
			FileUtil.copy(srcFS, src, dstFS, dst, deleteSource,  conf);
		} else{
			// Copy HFileLink
			// TODO: make it more robust
			Path src = ((HFileLink) file).getAvailablePath(srcFS);
			FileUtil.copy(srcFS, src, dstFS, dst, deleteSource,  conf);
		}
	}

    private String getSnapshotName()
    {
    	return backupItem.getBackupItem() + "_"+ backupItem.getBackup().getBackupTimestamp();
    }

}

