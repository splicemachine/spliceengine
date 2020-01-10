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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Random;

/**
 * Before creating tasks on multiple nodes use this class to verify that the export destination directory
 * can be created and written to.  If not throws StandardException with error message suitable for display to user.
 */
class ExportPermissionCheck {
    private static final Logger LOG = Logger.getLogger(ExportPermissionCheck.class);
    private ExportParams exportParams;
    private ExportFile testFile;

    ExportPermissionCheck(ExportParams exportParams,DistributedFileSystem dfs) {
        this.exportParams = exportParams;
        byte[] testFileTaskId = new byte[16];
        new Random().nextBytes(testFileTaskId);
        testFile = new ExportFile(exportParams, testFileTaskId,dfs);
    }

    ExportPermissionCheck(ExportParams exportParams) throws StandardException {
        this(exportParams, ImportUtils.getFileSystem(exportParams.getDirectory()));
    }

    void verify() throws StandardException {
        verifyExportDirExistsOrCanBeCreated();
        verifyExportDirWritable();
    }

    private void verifyExportDirExistsOrCanBeCreated() throws StandardException {
        boolean created = testFile.createDirectory();
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "verifyExportDirExistsOrCanBeCreated(): created=%s", created);
        if (!created) {
            throw StandardException.newException(SQLState.UU_INVALID_PARAMETER,
                    "cannot create export directory", exportParams.getDirectory());
        }
    }

    /* The FileSystem API allows us to query the permissions of a given directory but we are unable to evaluate those
     * permissions without the group membership of the current user.  Group membership in HDFS can be configured to
     * come from the underlying filesystem, or from an external source (LDAP).
     *
     * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html
     *
     * Given that limitation we instead check permissions by attempting to create a temporary file in the export
     * directory. Maybe future version of HDFS will implement .isWritable() or allow us to query group membership.
     */
    private void verifyExportDirWritable() throws StandardException {

        StandardException userVisibleErrorMessage = StandardException.newException(SQLState.UU_INVALID_PARAMETER,
                "cannot write to export directory", exportParams.getDirectory()); // TODO: i18n

        boolean writable = testFile.isWritable();
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "verifyExportDirWritable(): writable=%s", writable);
        if(!writable){
            throw userVisibleErrorMessage;
        }
    }

    public void cleanup() throws IOException {
        testFile.delete();
        // DB-5027: DO NOT delete the directory. The directory might contain important files
        // unrelated to export. Instead, just "clear" the directory of prior export files.
        // testFile.deleteDirectory();
        testFile.clearDirectory();
    }
}
