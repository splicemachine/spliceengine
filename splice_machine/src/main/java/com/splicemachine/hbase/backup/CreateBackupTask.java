package com.splicemachine.hbase.backup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hadoopbackport.ThrottledInputStream;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot.Counter;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;

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
	
	private static final String CONF_BANDWIDTH_MB = "splice.backup.bandwidth.mb";
	
	final static int REPORT_SIZE = 4 * 1024 * 1024;
    final static int BUFFER_SIZE = 64 * 1024;
    
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
            SpliceLogUtils.trace(LOG, String.format("executing %S backup on region %s",backupItem.getBackup().isIncrementalBackup()?"incremental":"full", region.toString()));
        try {
            writeRegionInfoOnFilesystem();
            doFullBackup();
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private void writeRegionInfoOnFilesystem() throws IOException
    {
    	DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
        FileSystem fs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);

        BackupUtils.derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(),
                new Path(backupFileSystem), fs, SpliceConstants.config);
    }
    
    private void doFullBackup() throws IOException
    {
    	SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
    	// TODO
    	// Make sure that backup directory structure is
    	// backupId/namespace/table/region/cf
    	// files can be HFile links or real paths to a 
    	// materialized references
    	List<Object> files = utils.getFilesForFullBackup(getSnapshotName(), region);
    	//String backupDirectory = backupItem.getBackupItemFilesystem();
    	//String name = region.getRegionNameAsString();
    	FileSystem backupFs = FileSystem.get(URI.create(backupFileSystem), SpliceConstants.config);
    	for(Object file: files){
            FileSystem fs = region.getFilesystem();
            String[] s = file.toString().split("/");
            int n = s.length;
            String fileName = s[n - 1];
            String familyName = s[n - 2];
            String regionName = s[n - 3];
            Path destPath = new Path(backupFileSystem + "/" + regionName + "/" + familyName + "/" + fileName);
            copyFileWithThrottling(fs, file, backupFs,  destPath, false, SpliceConstants.config);
	    	if(isTempFile(file)){
	    		deleteFile(fs, file, false);
	    	}
    	}
    }
    
    private void deleteFile(FileSystem fs, Object file, boolean b) throws IOException {
		fs.delete((Path) file, b);
	}

	@SuppressWarnings("unused")
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

	private Configuration getConfiguration()
	{
		return SpliceConstants.config;
	}
	
	private Path getPath(FileSystem fs, Object file) throws IOException{
		if( file instanceof HFileLink) {
			HFileLink link = (HFileLink) file;
			return link.getAvailablePath(fs);
		} else{
			return (Path) file;
		}
	}
	
	private void copyFileWithThrottling(FileSystem srcFS, Object file, FileSystem outputFs,
			Path outputPath, boolean deleteSource, Configuration conf) throws IOException 
	{
	      // Get the file information
	      FileStatus inputStat = getSourceFileStatus(srcFS, file);
		  // Verify if the output file exists and is the same that we want to copy
	      if (outputFs.exists(outputPath)) {
	        FileStatus outputStat = outputFs.getFileStatus(outputPath);
	        if (outputStat != null && sameFile(inputStat, outputStat)) {
	        	SpliceLogUtils.info(LOG, 
	        			"Skip copy " + inputStat.getPath() + " to " + outputPath + ", same file.");	          
	          return;
	        }
	      }
	      InputStream in = openSourceFile(srcFS, file);
	      int bandwidthMB = getConfiguration().getInt(CONF_BANDWIDTH_MB, 100);
	      if (Integer.MAX_VALUE != bandwidthMB) {
	        in = new ThrottledInputStream(new BufferedInputStream(in), bandwidthMB * 1024 * 1024);
	      }
	      try {
	        // Ensure that the output folder is there and copy the file
	        outputFs.mkdirs(outputPath.getParent());
	        FSDataOutputStream out = outputFs.create(outputPath, true);
	        try {
	          copyData( getPath(srcFS, file), in, outputPath, out, inputStat.getLen());
	        } finally {
	          out.close();
	        }
	      } finally {
	        in.close();
	      }		
	}
	

      /**
       * Check if the two files are equal by looking at the file length.
       * (they already have the same name).
       * 
       */
      private boolean sameFile(final FileStatus inputStat, final FileStatus outputStat) {
        // Not matching length
        if (inputStat.getLen() != outputStat.getLen()) return false;
        return true;
      }

	private FileStatus getSourceFileStatus(FileSystem fs, Object file) throws IOException {
		if( file instanceof HFileLink){
			HFileLink link = (HFileLink) file;
			return link.getFileStatus(fs);
		} else{
			return fs.getFileStatus((Path) file);
		}		
	}

	private FSDataInputStream openSourceFile(FileSystem fs, Object file) throws IOException
	{
		if( file instanceof HFileLink){
			return ((HFileLink) file).open(fs, BUFFER_SIZE);
		} else{
			Path path = (Path) file;
			return fs.open(path, BUFFER_SIZE);
		}
	}
	
    @SuppressWarnings("deprecation")
	private void copyData(
            final Path inputPath, final InputStream in,
            final Path outputPath, final FSDataOutputStream out,
            final long inputFileSize)
            throws IOException {
          final String statusMessage = "copied %s/" + 
        		  StringUtils.humanReadableInt(inputFileSize) + " (%.1f%%)";

          try {
            byte[] buffer = new byte[BUFFER_SIZE];
            long totalBytesWritten = 0;
            int reportBytes = 0;
            int bytesRead;

            long stime = System.currentTimeMillis();
            while ((bytesRead = in.read(buffer)) > 0) {
              out.write(buffer, 0, bytesRead);
              totalBytesWritten += bytesRead;
              reportBytes += bytesRead;
              if (reportBytes >= REPORT_SIZE) {
                
            	if (LOG.isTraceEnabled())
                      SpliceLogUtils.trace(LOG,
                       String.format(statusMessage,
                                  StringUtils.humanReadableInt(totalBytesWritten),
                                  (totalBytesWritten/(float)inputFileSize) * 100.0f) +
                                  " from " + inputPath + " to " + outputPath);
                reportBytes = 0;
              }
            }
            long etime = System.currentTimeMillis();
 
            SpliceLogUtils.info(LOG, String.format(statusMessage,
                    StringUtils.humanReadableInt(totalBytesWritten),
                    (totalBytesWritten/(float)inputFileSize) * 100.0f) +
                    " from " + inputPath + " to " + outputPath);
            
            // Verify that the written size match
            if (totalBytesWritten != inputFileSize) {
              String msg = "number of bytes copied not matching copied=" + totalBytesWritten +
                           " expected=" + inputFileSize + " for file=" + inputPath;
              throw new IOException(msg);
            }

            SpliceLogUtils.info(LOG, "copy completed for input=" + inputPath + " output=" + outputPath);
            SpliceLogUtils.info(LOG, "size=" + totalBytesWritten +
                " (" + StringUtils.humanReadableInt(totalBytesWritten) + ")" +
                " time=" + StringUtils.formatTimeDiff(etime, stime) +
                String.format(" %.3fM/sec", (totalBytesWritten / ((etime - stime)/1000.0))/1048576.0));
          } catch (IOException e) {
            LOG.error("Error copying " + inputPath + " to " + outputPath, e);
            throw e;
          }
        }
	
	/**
     * TODO: do we have to delete temporary files (materialized from refs)?    
     * @param file
     * @return true if temporary file
     */
	private boolean isTempFile(Object file) {		
		return file instanceof Path;
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

