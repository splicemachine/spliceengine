package com.splicemachine.derby.impl.sql;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class NoOpBackupManager implements BackupManager{
    private static NoOpBackupManager ourInstance=new NoOpBackupManager();

    public static NoOpBackupManager getInstance(){
        return ourInstance;
    }

    private NoOpBackupManager(){ }

    @Override
    public void fullBackup(String backupDirectory) throws StandardException {
//        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void incrementalBackup(String directory) throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public long getRunningBackup() throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void restoreDatabase(String directory,long backupId) throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void removeBackup(long backupId) throws StandardException{
//        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void scheduleDailyBackup(String directory, String type, int hour) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void cancelDailyBackup(long jobId) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void cancelBackup() throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }
}
