package com.splicemachine.derby.impl.sql;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.backup.RestoreItem;

import java.io.IOException;
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
    public void fullBackup(String backupDirectory) throws IOException{

    }

    @Override
    public void incrementalBackup(String directory) throws IOException{

    }

    @Override
    public String getRunningBackup(){
        return null;
    }

    @Override
    public void restoreDatabase(String directory,long backupId) throws IOException{

    }

    @Override
    public void removeBackup(long backupId) throws IOException{

    }

    @Override
    public Iterator<RestoreItem> listRestoreItems() throws IOException{
        return Collections.emptyIterator();
    }
}
