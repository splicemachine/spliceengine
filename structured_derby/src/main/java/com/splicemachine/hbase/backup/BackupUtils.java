package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;

public class BackupUtils {
	public static final String REGION_INFO = "region.info";
	public static boolean createBackupTable() {
		return true;
	}
	
	public static String isBackupRunning() throws SQLException {
		return Backup.isBackupRunning();
	}
	

	/**
	 * Write Region to Backup Directory
	 * 
	 * @param region
	 * @param backupDirectory
	 * @param backupFileSystem
	 * @throws ExecutionException
	 */
	public static void writeRegionToBackupDirectory(HRegion region, String backupDirectory, FileSystem backupFileSystem) throws ExecutionException {
    	try{	        	
            region.flushcache();
            region.startRegionOperation();
            FileSystem fs = region.getFilesystem();
	    	FileUtil.copy(fs, region.getRegionDir(), fs, new Path(backupDirectory+"/"+region.getRegionDir().getName()), false, SpliceConstants.config);
	    	HRegion.writeRegioninfoOnFilesystem(region.getRegionInfo(), new Path(backupDirectory+"/"+region.getRegionDir().getName()+"/"+REGION_INFO), backupFileSystem, SpliceConstants.config);
        } catch (Exception e) {
        	region.closeRegionOperation();
            throw new ExecutionException(Throwables.getRootCause(e));
        } finally {
        	region.closeRegionOperation();
        }

	}
	
	public static void createBackupTables() throws SQLException {
		Backup.createBackupSchema();
		Backup.createBackupTable();
		BackupItem.createBackupItemTable();
	}
	
}
