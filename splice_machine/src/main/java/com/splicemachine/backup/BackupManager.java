/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.backup;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface BackupManager{

    long fullBackup(String backupDirectory, boolean sync) throws StandardException;

    long incrementalBackup(String directory, boolean sync) throws StandardException;

    void restoreDatabase(String directory,long backupId, boolean sync, boolean validate, String timestamp, long txnId)throws StandardException;

    void removeBackup(List<Long> backupIds) throws StandardException;

    BackupJobStatus[] getRunningBackups() throws StandardException;

    void cancelBackup(long backupId) throws StandardException;

    void validateBackup(String directory,long backupId)throws StandardException;

    void validateSchemaBackup(String schemaName, String directory,long backupId)throws StandardException;

    void validateTableBackup(String schemaName, String tableName, String directory,long backupId)throws StandardException;

    void fullBackupTable(String schemaName, String tableName, String backupDirectory) throws StandardException;

    void fullBackupSchema(String schemaName, String backupDirectory) throws StandardException;

    void restoreTable(String destSchema, String destTable, String sourceSchema, String sourceTable, String directory,
                      long backupId, boolean validate) throws StandardException;

    void restoreSchema(String destSchema, String sourceSchema, String directory,
                      long backupId, boolean validate) throws StandardException;
}
