package com.splicemachine.hbase.backup;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.*;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);

    public static final String QUERY_LAST_BACKUP = "select max(backup_transaction_id) from %s.%s";
    public static final String CREATE_FILESET_TABLE = "create table %s.%s(CONGLOMERATE_ID bigint, REGION VARCHAR(1024), NAME VARCHAR(1024))";
    public static final String QUERY_FILE_SET = "select * from %s.%s where conglomerate_Id=? and region=? and name=?";
    public static final String DELETE_FILE_SET = "delete from %s.%s where conglomerate_Id=? and region=? and name=?";
    public static final String INSERT_FILESET = "insert into %s.%s values(?, ?, ?)";
    public static final String FILESET_SCHEMA = "BACKUP";
    public static final String FILESET_TABLE = "FILESET";
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
	public static final String REGION_INFO = "region.info";
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
            // Copy archived HFile
            copyArchivedStoreFile(region, backupItem);
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

    private static void copyArchivedStoreFile(HRegion region, BackupItem backupItem) throws ExecutionException {

        String backupDirectory = backupItem.getBackupItemFilesystem();
        try {
            Path archiveDir = HFileArchiveUtil.getArchivePath(SpliceConstants.config);
            String encodedRegionName = region.getRegionInfo().getEncodedName();
            Long conglomId = new Long(backupItem.getBackupItem());
            Path path = new Path(archiveDir + "/data/default/" + backupItem.getBackupItem() + "/" + encodedRegionName);
            FileSystem fileSystem = FileSystem.get(URI.create(path.toString()),SpliceConstants.config);
            FileStatus[] status = fileSystem.listStatus(path);
            for (FileStatus stat : status) {
                //For each column family
                if (!stat.isDirectory()) {
                    continue;
                }
                String family = stat.getPath().getName();
                FileStatus[] fileStatuses = fileSystem.listStatus(stat.getPath());
                for(FileStatus fs : fileStatuses) {
                    Path srcPath = fs.getPath();
                    String fileName = srcPath.getName();
                    Path destPath = new Path(backupDirectory+"/"+ encodedRegionName + "/" + family + "/" + fileName);
                    if (retainedForBackup(conglomId, encodedRegionName, fileName)) {
                        FileUtil.copy(fileSystem, srcPath, fileSystem, destPath, false, SpliceConstants.config);
                        deleteFileSetTable(conglomId, encodedRegionName, fileName);
                    }
                }
            }

        }
        catch(Exception e) {
            //throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private static void deleteFileSetTable(long conglomId, String encodedRegionName, String fileName) throws ExecutionException {
        Connection connection = null;

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(DELETE_FILE_SET, FILESET_SCHEMA, FILESET_TABLE));
            ps.setLong(1, conglomId);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.execute();
        }
        catch (SQLException e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    public static boolean retainedForBackup(long conglomId, String encodedRegionName, String fileName){
        Connection connection = null;

        try{
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(BackupUtils.QUERY_FILE_SET, BackupUtils.FILESET_SCHEMA, BackupUtils.FILESET_TABLE));
            ps.setLong(1, conglomId);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        }
        catch (SQLException e) {
            return false;
        }

        return false;
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

    public static void createFileSetTable() throws SQLException {
        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(CREATE_FILESET_TABLE, FILESET_SCHEMA, FILESET_TABLE));
            preparedStatement.execute();
            return;
        } catch (SQLException e) {
            throw e;
        }
        finally {
            if (connection !=null)
                connection.close();
        }
    }

    public static long getLastBackupTime() throws SQLException{

        long backupTransactionId = -1;
        Connection connection = null;
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(BackupUtils.QUERY_LAST_BACKUP, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                backupTransactionId = rs.getLong(1);
            }
        }
        catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot query last backup");
        }
        return backupTransactionId;
    }

    public static void recordStoreFile(long conglomerateId, String region, String path) throws SQLException {
        Connection connection = null;
        connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                String.format(BackupUtils.INSERT_FILESET, FILESET_SCHEMA, FILESET_TABLE));
        preparedStatement.setLong(1, conglomerateId);
        preparedStatement.setString(2, region);
        preparedStatement.setString(3, path);
        preparedStatement.execute();

    }

}
