package com.splicemachine.derby.impl.sql;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.backup.RestoreItem;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.Collections;
import java.util.Iterator;

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

    }

    @Override
    public void incrementalBackup(String directory) throws StandardException{

    }

    @Override
    public long getRunningBackup() throws StandardException{
        return 0;
    }

    @Override
    public void restoreDatabase(String directory,long backupId) throws StandardException{

    }

    @Override
    public void removeBackup(long backupId) throws StandardException{

    }

    @Override
    public void scheduleDailyBackup(String directory, String type, int hour) throws StandardException {

    }

    @Override
    public void cancelDailyBackup(long jobId) throws StandardException {

    }
}
