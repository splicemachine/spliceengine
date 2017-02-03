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
import com.splicemachine.si.impl.TestingFileSystem;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.junit.Assert.assertEquals;

@Category(ArchitectureIndependent.class)
public class ExportPermissionCheckTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DistributedFileSystem dfs = new TestingFileSystem(FileSystems.getDefault().provider());
    @BeforeClass
    public static void setupConfig() {
        // necessary for mapr
//        SpliceConstants.config.set("fs.default.name", "file:///");
    }

    @Test
    public void verify() throws IOException, StandardException {
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportPermissionCheck permissionCheck = new ExportPermissionCheck(exportParams,dfs);

        // No exception, we can write to the temp folder
        permissionCheck.verify();
        assertEquals("export dir should be empty after check", 0, temporaryFolder.getRoot().listFiles().length);
    }

    @Test
    public void verify_failCase() throws IOException, StandardException {
        expectedException.expect(StandardException.class);
        expectedException.expectMessage("IOException '/ExportPermissionCheckTest' when accessing directory");

        ExportParams exportParams = ExportParams.withDirectory("/ExportPermissionCheckTest");
        ExportPermissionCheck permissionCheck = new ExportPermissionCheck(exportParams,dfs);
        permissionCheck.verify();
    }

}