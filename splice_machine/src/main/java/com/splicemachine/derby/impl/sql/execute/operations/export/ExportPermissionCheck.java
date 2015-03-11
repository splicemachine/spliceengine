package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;

/**
 * Before creating tasks on multiple nodes use this class to verify that the export destination directory
 * can be created and written to.  If not throws StandardException with error message suitable for display to user.
 */
class ExportPermissionCheck {

    private ExportParams exportParams;
    private ExportFile testFile;

    ExportPermissionCheck(ExportParams exportParams) throws IOException {
        this.exportParams = exportParams;
        byte[] testFileTaskId = new byte[16];
        new Random().nextBytes(testFileTaskId);
        testFile = new ExportFile(exportParams, testFileTaskId);
    }

    void verify() throws IOException, StandardException {
        verifyExportDirExistsOrCanBeCreated();
        verifyExportDirWritable();
    }

    private void verifyExportDirExistsOrCanBeCreated() throws StandardException {
        if (!testFile.createDirectory()) {
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

        try {
            Writer writer = new OutputStreamWriter(testFile.getOutputStream(), exportParams.getCharacterEncoding());
            writer.write(" ");
            writer.close();
        } catch (IOException e) {
            throw userVisibleErrorMessage;
        } finally {
            // no-op if file does not exist.  File will be empty (single space) in the event that
            // we can, for some reason, create it but not delete it. However documentation of HDFS permission model
            // seems to indicate that this should not be possible.
            try {
                testFile.delete();
            } catch (IOException io) {
                //noinspection ThrowFromFinallyBlock
                throw userVisibleErrorMessage;
            }
        }
    }
}
