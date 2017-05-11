/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.util.List;

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
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
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
    public void removeBackup(List<Long> backupIds) throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
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

    @Override
    public void post_restore_cleanup(long backupId) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }
}
