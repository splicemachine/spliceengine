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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.si.impl.driver.SIDriver;
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

    ExportPermissionCheck(ExportParams exportParams,DistributedFileSystem dfs) throws IOException {
        this.exportParams = exportParams;
        byte[] testFileTaskId = new byte[16];
        new Random().nextBytes(testFileTaskId);
        testFile = new ExportFile(exportParams, testFileTaskId,dfs);
    }

    ExportPermissionCheck(ExportParams exportParams) throws IOException {
        this(exportParams,SIDriver.driver().getFileSystem(exportParams.getDirectory()));
    }

    void verify() throws IOException, StandardException {
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
    private void verifyExportDirWritable() throws IOException, StandardException {

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
