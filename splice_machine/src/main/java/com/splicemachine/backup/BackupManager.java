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

    long getRunningBackup() throws StandardException;

    void restoreDatabase(String directory,long backupId)throws StandardException;

    void removeBackup(long backupId) throws StandardException;

    void scheduleDailyBackup(String directory, String type, int hour) throws StandardException;

    void cancelDailyBackup(long jobId) throws StandardException;

    void cancelBackup() throws StandardException;
}
