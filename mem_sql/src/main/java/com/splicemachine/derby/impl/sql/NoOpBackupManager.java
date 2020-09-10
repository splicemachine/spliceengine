/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.backup.BackupMessage.BackupJobStatus;
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
    public long fullBackup(String backupDirectory, boolean sync) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public long incrementalBackup(String directory, boolean sync) throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void restoreDatabase(String directory,long backupId, boolean sync, boolean validate, String timestamp, long txnId)throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void removeBackup(List<Long> backupIds) throws StandardException{
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public BackupJobStatus[] getRunningBackups() throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void cancelBackup(long backupId) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void validateBackup(String dir, long backupId) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void validateSchemaBackup(String schemaName, String directory,long backupId)throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }
    @Override
    public void validateTableBackup(String schemaName, String tableName, String directory,long backupId)throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void fullBackupTable(String schemaName, String tableName, String backupDirectory) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void fullBackupSchema(String schemaName, String backupDirectory) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void restoreTable(String destSchema, String destTable, String sourceSchema, String sourceTable,
                             String directory, long backupId, boolean validate) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }

    @Override
    public void restoreSchema(String destSchema, String sourceSchema, String directory,
                              long backupId, boolean validate) throws StandardException {
        throw StandardException.newException(SQLState.BACKUP_OPERATIONS_DISABLED);
    }
}
