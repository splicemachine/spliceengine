package com.splicemachine.backup;

import com.splicemachine.db.iapi.error.StandardException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface BackupManager{

    void fullBackup(String backupDirectory) throws StandardException;

    void incrementalBackup(String directory) throws StandardException;

    String getRunningBackup();

    void restoreDatabase(String directory,long backupId)throws StandardException;

    void removeBackup(long backupId) throws StandardException;

    Iterator<RestoreItem> listRestoreItems() throws StandardException;
}
