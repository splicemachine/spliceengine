package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ExportPermissionCheckTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void verify() throws IOException, StandardException {
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportPermissionCheck permissionCheck = new ExportPermissionCheck(exportParams);

        // No exception, we can write to the temp folder
        permissionCheck.verify();
        assertEquals("export dir should be empty after check", 0, temporaryFolder.getRoot().listFiles().length);
    }

    @Test
    public void verify_failCase() throws IOException, StandardException {
        expectedException.expect(StandardException.class);
        expectedException.expectMessage("Invalid parameter 'cannot create export directory'='/ExportPermissionCheckTest'.");

        ExportParams exportParams = ExportParams.withDirectory("/ExportPermissionCheckTest");
        ExportPermissionCheck permissionCheck = new ExportPermissionCheck(exportParams);
        permissionCheck.verify();
    }

}