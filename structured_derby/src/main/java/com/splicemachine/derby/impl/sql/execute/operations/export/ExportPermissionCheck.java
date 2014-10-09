package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

import java.io.IOException;
import java.io.OutputStream;
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

    private void verifyExportDirWritable() throws IOException, StandardException {
        try {
            OutputStream outputStream = testFile.getOutputStream();
            outputStream.close();
        } catch (IOException e) {
            throw StandardException.newException(SQLState.UU_INVALID_PARAMETER,
                    "cannot write to export directory", exportParams.getDirectory());
        } finally {
            // no-op if file does not exist.  File will be empty in the event that
            // we can, for some reason, create it but not delete it.
            testFile.delete();
        }
    }
}
