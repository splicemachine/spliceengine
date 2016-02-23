package com.splicemachine.derby.lifecycle;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.backup.BackupUtils;
import com.splicemachine.backup.RestoreItem;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.Iterator;

/**
 * Created by jyuan on 2/21/16.
 */
public class SpliceBackupManager implements BackupManager {

    @Override
    public void fullBackup(String directory) throws StandardException {
        BackupUtils.fullBackup(directory);
    }

    @Override
    public void incrementalBackup(String directory) throws StandardException{
        BackupUtils.incrementalBackup(directory);
    }

    @Override
    public String getRunningBackup(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void restoreDatabase(String directory,long backupId) throws StandardException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void removeBackup(long backupId) throws StandardException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Iterator<RestoreItem> listRestoreItems() throws StandardException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
