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
import com.splicemachine.si.impl.TestingFileSystem;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.stringContainsInOrder;
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

    @Ignore
    @Test
    public void verify_failCase() throws IOException, StandardException {
        ExportParams exportParams = ExportParams.withDirectory("/ExportPermissionCheckTest");
        ExportPermissionCheck permissionCheck = new ExportPermissionCheck(exportParams,dfs);
        try {
            permissionCheck.verify();
        } catch (Exception e) {
            assertThat(e.getMessage(),
                    stringContainsInOrder("IOException '/ExportPermissionCheckTest", "' when accessing directory"));
        }
    }

}
