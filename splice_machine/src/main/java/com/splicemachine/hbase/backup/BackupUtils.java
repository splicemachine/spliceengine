package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.*;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import com.splicemachine.si.api.TxnView;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import com.splicemachine.db.iapi.error.ShutdownException;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);
    public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final int MAX_RETRIES = 30;

    public static String isBackupRunning() throws KeeperException, InterruptedException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        if (zooKeeper.exists(SpliceConstants.DEFAULT_BACKUP_PATH, false) != null) {
            long id = BytesUtil.bytesToLong(zooKeeper.getData(SpliceConstants.DEFAULT_BACKUP_PATH, false, null), 0);
            return String.format("A concurrent backup with id of %d is running.", id);
        }
        return null;
    }

    /**
     *
     * @param region HBase region
     * @return store files for each column family
     * @throws ExecutionException
     */
    public static HashMap<byte[], Collection<StoreFile>> getStoreFiles(HRegion region) throws ExecutionException {
        try {
            HashMap<byte[], Collection<StoreFile>> storeFiles = new HashMap<>();
            Map<byte[],Store> stores = region.getStores();

            for (byte[] family : stores.keySet()) {
                Store store = stores.get(family);
                Collection<StoreFile> storeFileList = store.getStorefiles();
                storeFiles.put(family, storeFileList);
            }
            return storeFiles;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    /**
     * Get directory of the specified backup
     * @param parent_backup_id
     * @return
     * @throws StandardException
     */
    public static String getBackupDirectory(long parent_backup_id, TxnView txn) throws StandardException {

        String dir = null;

        if (parent_backup_id == -1) {
            return null;
        }
        Backup backup = BackupSystemProcedures.backupReporter.getBackup(parent_backup_id, txn);
        if (backup != null) {
            dir = backup.getBackupFilesystem();
        }
        return dir;
    }

    /**
     * Get backup Id for the latest backup
     * @return backup Id
     * @throws SQLException
     */
    public static long  getLastBackupId() throws StandardException {

        long backupId = -1;
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            BackupSystemProcedures.backupReporter.openScanner(txn);
            Backup backup = null;
            Backup.BackupStatus status = Backup.BackupStatus.F;
            do {
                backup = BackupSystemProcedures.backupReporter.next();

                if (backup != null) {
                    backupId = backup.getBackupId();
                    status = backup.getBackupStatus();
                }
                else {
                    backupId = -1;
                }
            } while (backup != null && status != Backup.BackupStatus.S);
            BackupSystemProcedures.backupReporter.closeScanner();
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            } catch (Exception ex) {
                throw Exceptions.parseException(ex);
            }
            throw Exceptions.parseException(e);
        }
        return backupId;
    }

    /**
     * Query BACKUP.BACKUP table, return true if there exists an entry with the specified status
     * @param status Back up status, "S", "I", or "F"
     * @return true if there exists an entry with the specified status
     */
    public static boolean existBackupWithStatus(String status, int retries) throws StandardException{

        Txn txn = null;
        Backup backup = null;

        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            BackupSystemProcedures.backupReporter.openScanner(txn);
            do {
                backup = BackupSystemProcedures.backupReporter.next();
            } while (backup != null && status.compareToIgnoreCase(backup.getBackupStatus().toString()) != 0);
            BackupSystemProcedures.backupReporter.closeScanner();
        }
        catch (ShutdownException e) {
            retries++;
            if (retries < MAX_RETRIES) {
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                    throw Exceptions.parseException(ex);
                }
                return existBackupWithStatus(status, retries);
            }
            else {
                throw Exceptions.parseException(e);
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }

        return backup != null;
    }

    /**
     * Get last snapshot name.
     * @param tableName
     * @return last snapshot name
     */
    public static String getLastSnapshotName(String tableName) {

        String snapshotName = null;
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtilities.getAdmin();
            long creationTime = 0;
            List<HBaseProtos.SnapshotDescription> snapshotDescriptionList = admin.listSnapshots(tableName+"_\\d+$");
            for (HBaseProtos.SnapshotDescription snapshotDescription : snapshotDescriptionList) {
                if(snapshotDescription.getCreationTime() > creationTime) {
                    snapshotName = snapshotDescription.getName();
                }
            }
        } catch (IOException e) {
          return null;
        } finally {
        Closeables.closeQuietly(admin);
    }
        return snapshotName;
    }

    public static long getBackupId(String snapshotName) {
        String s[] = snapshotName.split("_");
        return new Long(s[1]);
    }

    public static Backup getBackup(long backupId) throws StandardException{
        Txn txn = null;
        Backup backup = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            backup = BackupSystemProcedures.backupReporter.getBackup(backupId, txn);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }

        return backup;
    }

    /**
     * A reference file should be excluded for next incremental backup if
     * 1) it appears in the last snapshot, or
     * 2) it references
     * @param tableName name of HBase table
     * @param referenceFileName name of reference file
     * @param fs file system
     * @return true, if the file is a reference file, and should not be included in next incremental backup
     * @throws IOException
     */
    public static boolean shouldExcludeReferencedFile(String tableName,
                                                      String referenceFileName,
                                                      FileSystem fs) throws IOException, StandardException{
        String[] s = referenceFileName.split("\\.");
        int n = s.length;
        if (n == 1) {
            // This is not a reference file
            return false;
        }
        String fileName = s[0];
        String encodedRegionName = s[1];
        SnapshotUtils snapshotUtils = SnapshotUtilsFactory.snapshotUtils;

        // Get last snapshot for this table
        Configuration conf = SpliceConstants.config;
        String snapshotName = BackupUtils.getLastSnapshotName(tableName);
        Set<String> pathSet = new HashSet<>();
        if (snapshotName != null) {
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG, "checking snapshot: " + snapshotName);
            }
            List<Object> pathList = snapshotUtils.getSnapshotFilesForRegion(null, conf, fs, snapshotName, false);
            for (Object obj : pathList) {
                Path path = ((HFileLink) obj).getAvailablePath(fs);
                if (LOG.isTraceEnabled()) {
                    SpliceLogUtils.trace(LOG, "snapshot contains file " + path.toString());
                }
                pathSet.add(path.getName());
            }
        }

        if(pathSet.contains(fileName)) {
            // If the referenced file appears in last snapshot, this file should be excluded for next incremental
            // backup
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG, "snapshot contains file " + s[0]);
            }
            return true;
        }

        if (shouldExclude(tableName, encodedRegionName, fileName)) {
            // Check BACKUP.BACKUP_FILESET whether this file should be excluded
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG, "Find an entry in Backup.Backup_fileset for table = " + tableName +
                        " region = " + s[1] + " file = " + s[0]);
            }
            return true;
        }

        return false;
    }

    /**
     *
     * @param tableName name of table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @return true, if the HFile should not be included in the next incremental backup
     */
    public static boolean shouldExclude(String tableName,
                                        String encodedRegionName,
                                        String fileName) throws StandardException{

        boolean exclude = false;

        BackupFileSetReporter backupFileSetReporter = null;
        BackupFileSet backupFileSet = null;
        try {
            backupFileSetReporter = scanFileSetForRegion(tableName, encodedRegionName);
            while((backupFileSet = backupFileSetReporter.next()) != null) {
                if (backupFileSet.getFileName().compareToIgnoreCase(fileName) == 0 &&
                    backupFileSet.shouldInclude() == false) {
                    exclude = true;
                    break;
                }
            }
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        } finally {
            backupFileSetReporter.closeScanner();
        }

        return exclude;
    }

    /**
     * Insert an entry to BACKUP.BACKUP_FILESET
     * @param tableName name of name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @param include whether this file should be included in next incremental backup
     */
    public static void insertFileSet(String tableName,
                                     String encodedRegionName,
                                     String fileName,
                                     boolean include) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            BackupFileSet backupFileSet = new BackupFileSet(tableName, encodedRegionName, fileName, include);
            BackupSystemProcedures.backupFileSetReporter.report(backupFileSet, txn);
            if (LOG.isTraceEnabled()) {
                String record = String.format("(%s, %s, %s, %s)", tableName, encodedRegionName, fileName, include);
                LOG.trace("inserted " + record + " to sys.sysbackupfileset");
            }
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }
            catch (Exception ex) {
                throw Exceptions.parseException(ex);
            }
            throw Exceptions.parseException(e);
        }
    }

    /**
     * Query BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @return
     */
    public static BackupFileSetReporter scanFileSetForRegion(String tableName,
                                                             String encodedRegionName) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction(Txn.IsolationLevel.READ_COMMITTED);
            BackupSystemProcedures.backupFileSetReporter.openScanner(txn, tableName, encodedRegionName);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
        return BackupSystemProcedures.backupFileSetReporter;
    }

    /**
     * Delete entries from BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @param include whether this file should be included in next incremental backup
     */
    public static void deleteFileSet(String tableName,
                                     String encodedRegionName,
                                     String fileName,
                                     boolean include) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            BackupFileSet backupFileSet = new BackupFileSet(tableName, encodedRegionName, fileName, include);
            BackupSystemProcedures.backupFileSetReporter.remove(backupFileSet, txn);
            if (LOG.isTraceEnabled()) {
                String record = String.format("(%s, %s, %s, %s)", tableName, encodedRegionName, fileName, include);
                LOG.trace("removed " + record + " from sys.sysbackupfileset");
            }
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }catch(Exception ex) {
                throw Exceptions.parseException(ex);
            }
            throw Exceptions.parseException(e);
        }
    }

    public static void deleteBackup(long backupId) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            Backup backup = new Backup();
            backup.setBackupId(backupId);
            BackupSystemProcedures.backupReporter.remove(backup, txn);
            if (LOG.isTraceEnabled()) {
                String record = String.format("(%d)", backupId);
                LOG.trace("removed " + record + " from sys.sysbackup");
            }
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }catch(Exception ex) {
                throw Exceptions.parseException(ex);
            }
            throw Exceptions.parseException(e);
        }
    }

    public static void deleteBackupItem(long backupId) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            BackupSystemProcedures.backupItemReporter.openScanner(txn, backupId);
            BackupItem backupItem = null;
            while((backupItem = BackupSystemProcedures.backupItemReporter.next()) != null) {
                BackupSystemProcedures.backupItemReporter.remove(backupItem, txn);
            }
            if (LOG.isTraceEnabled()) {
                String record = String.format("(%d)", backupId);
                LOG.trace("removed " + record + " from sys.sysbackup");
            }
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }catch(Exception ex) {
                throw Exceptions.parseException(ex);
            }
            throw Exceptions.parseException(e);
        }
        finally {
            BackupSystemProcedures.backupItemReporter.closeScanner();
        }
    }
    /**
     * Look for an entry for an HFile in BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @return
     */
    public static BackupFileSet getFileSet(String tableName,
                                           String encodedRegionName,
                                           String fileName) throws StandardException {

        BackupFileSetReporter backupFileSetReporter = null;
        BackupFileSet backupFileSet = null;
        try {
            backupFileSetReporter = scanFileSetForRegion(tableName, encodedRegionName);
            while ((backupFileSet = backupFileSetReporter.next()) != null) {
                if (backupFileSet.getFileName().compareToIgnoreCase(fileName) == 0) {
                    break;
                }
            }
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        } finally {
            backupFileSetReporter.closeScanner();
        }
        return backupFileSet;
    }

    /**
     * TODO: do we have to delete temporary files (materialized from refs)?
     * @param file
     * @return true if temporary file
     */
    public static boolean isTempFile(Object file) {
        return file instanceof Path;
    }

    public static void deleteFile(FileSystem fs, Object file, boolean b) throws IOException {
        fs.delete((Path) file, b);
    }

    public static void copyHFileHalf(Configuration conf,
                                     Path inFile,
                                     Path outFile,
                                     Reference reference,
                                     HColumnDescriptor familyDescriptor) throws IOException {

        FileSystem fs = inFile.getFileSystem(conf);
        CacheConfig cacheConf = new CacheConfig(conf);
        HalfStoreFileReader halfReader = null;
        StoreFile.Writer halfWriter = null;
        int count = 0;
        try {
            halfReader = new HalfStoreFileReader(fs, inFile, cacheConf, reference, conf);
            Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

            int blocksize = familyDescriptor.getBlocksize();
            Compression.Algorithm compression = familyDescriptor.getCompression();
            BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
            HFileContext hFileContext = new HFileContextBuilder()
                    .withCompression(compression)
                    .withChecksumType(HStore.getChecksumType(conf))
                    .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                    .withBlockSize(blocksize)
                    .withDataBlockEncoding(familyDescriptor.getDataBlockEncoding())
                    .build();
            halfWriter = new StoreFile.WriterBuilder(conf, cacheConf,
                    fs)
                    .withFilePath(outFile)
                    .withBloomType(bloomFilterType)
                    .withFileContext(hFileContext)
                    .build();
            HFileScanner scanner = halfReader.getScanner(false, false, false);
            scanner.seekTo();
            do {
                count++;
                KeyValue kv = KeyValueUtil.ensureKeyValue(scanner.getKeyValue());
                halfWriter.append(kv);
            } while (scanner.next());

            for (Map.Entry<byte[],byte[]> entry : fileInfo.entrySet()) {
                if (shouldCopyHFileMetaKey(entry.getKey())) {
                    halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
                }
            }
        } finally {
            if (halfWriter != null) halfWriter.close();
            if (halfReader != null) halfReader.close(cacheConf.shouldEvictOnClose());
            SpliceLogUtils.trace(LOG, "materialized %d kvs to %s", count, outFile);
        }
    }

    private static boolean shouldCopyHFileMetaKey(byte[] key) {
        return !HFile.isReservedFileInfoKey(key);
    }

    public static Path getRandomFilename(final FileSystem fs,
                                  final Path dir)
            throws IOException {
        return new Path(dir, UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public static boolean isReference( String fileName) {
        return fileName.indexOf(".") > 0;
    }
}