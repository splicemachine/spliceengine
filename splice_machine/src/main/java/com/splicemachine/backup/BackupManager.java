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

package com.splicemachine.backup;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface BackupManager{

    void fullBackup(String backupDirectory) throws StandardException;

    void incrementalBackup(String directory) throws StandardException;

    long getRunningBackup() throws StandardException;

    void restoreDatabase(String directory,long backupId)throws StandardException;

    void removeBackup(List<Long> backupIds) throws StandardException;

    void scheduleDailyBackup(String directory, String type, int hour) throws StandardException;

    void cancelDailyBackup(long jobId) throws StandardException;

    void cancelBackup() throws StandardException;
}
