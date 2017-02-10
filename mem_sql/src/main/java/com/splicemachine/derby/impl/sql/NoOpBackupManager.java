/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
}
