package com.splicemachine.backup;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface BackupManager{

    void fullBackup(String backupDirectory) throws IOException;

    void incrementalBackup(String directory) throws IOException;

    String getRunningBackup();

    void restoreDatabase(String directory,long backupId)throws IOException;

    void removeBackup(long backupId) throws IOException;

    Iterator<RestoreItem> listRestoreItems() throws IOException;
}
