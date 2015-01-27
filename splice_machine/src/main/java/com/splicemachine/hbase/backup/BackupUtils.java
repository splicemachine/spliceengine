package com.splicemachine.hbase.backup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import com.splicemachine.derby.utils.SpliceAdmin;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.*;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

public class BackupUtils {
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
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
	public static void fullBackupRegion(HRegion region, String backupDirectory, FileSystem backupFileSystem) throws ExecutionException {
    	try{	        	
            region.flushcache();
            region.startRegionOperation();
            FileSystem fs = region.getFilesystem();
            
	    	FileUtil.copy(fs, derbyFactory.getRegionDir(region), backupFileSystem, new Path(backupDirectory+"/"+derbyFactory.getRegionDir(region).getName()), false, SpliceConstants.config);
	    	derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(), new Path(backupDirectory+"/"+derbyFactory.getRegionDir(region).getName()+"/"+REGION_INFO), backupFileSystem, SpliceConstants.config);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        } finally {
        	try {
        		region.closeRegionOperation();
        	} catch (Exception e) {
                throw new ExecutionException(Throwables.getRootCause(e));
        	}
        }

	}

    public static void incrementalBackupRegion(HRegion region, BackupItem backupItem, FileSystem backupFileSystem) throws ExecutionException{

        try {
            region.flushcache();
            region.startRegionOperation();

            String backupDirectory = backupItem.getBackupItemFilesystem();
            FileSystem fs = region.getFilesystem();
            HRegionFileSystem hRegionFileSystem = region.getRegionFileSystem();
            boolean copied = false;
            Collection<String> families = hRegionFileSystem.getFamilies();
            for (String family : families) {
                Collection<StoreFileInfo> storeFileInfos = hRegionFileSystem.getStoreFiles(family);
                if (storeFileInfos != null) {
                    for (StoreFileInfo storeFileInfo : storeFileInfos) {
                        StoreFile storeFile = new StoreFile(fs, storeFileInfo, SpliceConstants.config,
                                new CacheConfig(SpliceConstants.config), BloomType.NONE);
                        StoreFile.Reader reader = storeFile.createReader();
                        Long minTimeStamp = storeFile.getMinimumTimestamp();
                        Long maxTimeStamp = reader.getMaxTimestamp();
                        long lastBackupTimestamp = backupItem.getLastBackupTimestamp();
                        if (minTimeStamp > lastBackupTimestamp && maxTimeStamp > lastBackupTimestamp) {
                            copied = true;
                            Path srcPath = storeFileInfo.getPath();
                            String s = srcPath.getName();
                            String regionName = derbyFactory.getRegionDir(region).getName();
                            Path destPath = new Path(backupDirectory+"/"+ regionName + "/" + family + "/" + s);
                            FileUtil.copy(fs, srcPath, fs, destPath, false, SpliceConstants.config);
                        }
                    }
                }
            }
            if (copied) {
                derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(), new Path(backupDirectory+"/"+derbyFactory.getRegionDir(region).getName()+"/"+REGION_INFO), backupFileSystem, SpliceConstants.config);
            }
        }
        catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
        finally {
            try {
                region.closeRegionOperation();
            } catch (Exception e) {
                throw new ExecutionException(Throwables.getRootCause(e));
            }
        }
    }

	public static void createBackupTables() throws SQLException {
		Backup.createBackupSchema();
		Backup.createBackupTable();
		BackupItem.createBackupItemTable();
	}

    public static String getBackupDirectory(long parent_backup_id) throws StandardException, SQLException{

        if (parent_backup_id == -1) {
            return null;
        }
        String dir = null;
        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(Backup.QUERY_PARENT_BACKUP_DIRECTORY, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            preparedStatement.setLong(1, parent_backup_id);
            ResultSet rs = preparedStatement.executeQuery();

            if (rs.next()) {
                dir = rs.getString(1);
            }
            else
                throw StandardException.newException("Parent backup does not exist");
        } catch (Exception e) {
            throw e;
        }
        finally {
            if (connection !=null)
                connection.close();
        }

        return dir;
    }
}
