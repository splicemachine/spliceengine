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

package com.splicemachine.backup;

import com.splicemachine.db.iapi.error.StandardException;
import java.util.Iterator;
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

    void post_restore_cleanup(long backupId) throws StandardException;
}
