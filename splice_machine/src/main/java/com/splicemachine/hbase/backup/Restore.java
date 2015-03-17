package com.splicemachine.hbase.backup;

/**
 * Created by jyuan on 3/12/15.
 */

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import com.splicemachine.hbase.backup.BackupItem.RegionInfo;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class Restore {
    Txn restoreTransaction;
    private List<Backup> backups;
    Map<String, BackupItem> backupItems;

    public Restore() {
    }

    public void setRestoreTransaction(Txn restoreTransaction) {
        this.restoreTransaction = restoreTransaction;
    }

    public Txn getRestoreTransaction() {
        return restoreTransaction;
    }

    public Map<String, BackupItem> getBackupItems() {
        return backupItems;
    }

    public static Restore createRestore(long backupId) throws SQLException, StandardException{

        Restore restore = null;
        try {
            Txn restoreTxn = TransactionLifecycle.getLifecycleManager().
                    beginTransaction().elevateToWritable("recovery".getBytes());

            restore = new Restore();
            restore.setRestoreTransaction(restoreTxn);
            restore.readBackup(backupId);
            Backup lastBackup = restore.getLastBackup();
            lastBackup.restoreProperties();
            lastBackup.restoreMetadata();
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return restore;
    }

    public Backup getLastBackup() {
        return backups.get(0);
    }

    private void combineBackups(Backup b1, Backup b2) {
        Map<String, BackupItem> backupItems1 = b1.getBackupItems();
        Map<String, BackupItem> backupItems2 = b2.getBackupItems();

        for(String item : backupItems1.keySet()) {
            if (!backupItems2.containsKey(item)) {
                backupItems2.put(item, backupItems1.get(item));
            }
            else {
                BackupItem item1 = backupItems1.get(item);
                BackupItem item2 = backupItems2.get(item);
                List<BackupItem.RegionInfo> regionInfoList1 = item1.getRegionInfoList();
                List<BackupItem.RegionInfo> regionInfoList2 = item2.getRegionInfoList();

                for(BackupItem.RegionInfo regionInfo1 : regionInfoList1) {
                    boolean fallsInto = false;
                    for (BackupItem.RegionInfo regionInfo2 : regionInfoList2) {
                        if (regionFallsInto(regionInfo1, regionInfo2)) {
                            List<Pair<byte[], String>> famPathList = regionInfo2.getFamPaths();
                            for(Pair<byte[], String> p : regionInfo1.getFamPaths()) {
                                famPathList.add(p);
                            }
                            fallsInto = true;
                            break;
                        }
                    }
                    if (!fallsInto) {
                        regionInfoList2.add(regionInfo1);
                    }
                }
            }
        }
    }

    private int compareStartKey(byte[] k1, byte[] k2) {
        int result = 0;
        Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();
        if (k1 == null && k2 == null ||
            k1.length == 0 && k2.length == 0) {
            result = 0;
        }
        else if (k1== null && k2 != null ||
                 k1.length == 0 && k2.length != 0) {
            result = -1;
        }
        else if (k1!= null && k2 == null ||
                 k1.length != 0 && k2.length == 0) {
            result = 1;
        }
        else {
            result = comparator.compare(k1, k2);
        }

        return result;
    }

    private int compareEndKey(byte[] k1, byte[] k2) {
        int result = 0;
        Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();
        if (k1 == null && k2 == null ||
                k1.length == 0 && k2.length == 0) {
            result = 0;
        }
        else if (k1 == null && k2 != null ||
                k1.length == 0 && k2.length != 0) {
            result = 1;
        }
        else if (k1 != null && k2 == null ||
                k1.length != 0 && k2.length == 0) {
            result = -1;
        }
        else {
            result = comparator.compare(k1, k2);
        }

        return result;
    }


    private boolean regionFallsInto(BackupItem.RegionInfo regionInfo1, BackupItem.RegionInfo regionInfo2) {
        byte[] startKey1 = regionInfo1.getHRegionInfo().getStartKey();
        byte[] endKey1 = regionInfo1.getHRegionInfo().getEndKey();
        byte[] startKey2 = regionInfo2.getHRegionInfo().getStartKey();
        byte[] endKey2 = regionInfo2.getHRegionInfo().getEndKey();

        return (compareStartKey(startKey1, startKey2) >= 0 && compareEndKey(endKey1, endKey2) <= 0);
    }

    private void readBackupItems() {

        Iterator<Backup> itr = backups.iterator();
        Backup backup = itr.next();
        while(itr.hasNext()) {
            Backup b = itr.next();
            combineBackups(backup, b);
            backup = b;
        }
        backupItems = backup.getBackupItems();
    }

    private void readBackup(long backupId) throws StandardException{
        try {
            createBackups(backupId);
            for (Backup backup : backups) {
                backup.readBackupItems();
            }
            readBackupItems();
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    private void createBackups(long backupId) throws StandardException, SQLException {
        String sqlText = "select transaction_id, incremental_parent_backup_id, filesystem from %s.%s where " +
                         "transaction_id<=? order by transaction_id desc";

        Connection connection = null;
        LinkedList<Backup> backupList = new LinkedList<>();

        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(sqlText, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            preparedStatement.setLong(1, backupId);
            ResultSet rs = preparedStatement.executeQuery();

            long prev = 0;
            long parentId = 0;
            while (rs.next()) {
                long id = rs.getLong(1);
                parentId = rs.getLong(2);
                String fileSystem = rs.getString(3);
                if (prev == 0 || id == prev) {
                    if (prev == 0 && id != backupId) {
                        throw StandardException.newException("Backup " + backupId + " does not exist");
                    }
                    Backup backup = new Backup();
                    backup.setBackupTransaction(restoreTransaction);
                    backup.setBackupId(id);
                    backup.setBackupScope(Backup.BackupScope.D);
                    backup.setBackupStatus(Backup.BackupStatus.I);
                    backup.setBackupFilesystem(fileSystem);
                    backupList.add(backup);
                    prev = parentId;
                    if (parentId == -1) break;
                }
            }
            if (parentId != -1) {
                throw StandardException.newException("Can not find a full Backup.");
            }
            backups = backupList;
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        } finally {
            if (connection != null)
                connection.close();
        }
    }
}
