package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.constants.SpliceConstants;
import org.apache.derby.iapi.error.StandardException;
import org.junit.BeforeClass;
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

    @BeforeClass
    public static void setupConfig() {
        // necessary for mapr
        SpliceConstants.config.set("fs.default.name", "file:///");
    }

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