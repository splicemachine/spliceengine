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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.si.impl.TestingFileSystem;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.si.testenv.SITestDataEnv;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;

import static org.junit.Assert.*;

@Category(ArchitectureIndependent.class)
public class ExportFileTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setupConfig() {
        // necessary for mapr
//        SpliceConstants.config.set("fs.default.name", "file:///");
    }

    private DistributedFileSystem dfs = new TestingFileSystem(FileSystems.getDefault().provider());

    @Test
    public void getOutputStream_createsStreamConnectedToExpectedFile() throws IOException {

        // given
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportFile exportFile = new ExportFile(exportParams, testTaskId(),dfs);
        final byte[] EXPECTED_CONTENT = ("splice for the win" + RandomStringUtils.randomAlphanumeric(1000)).getBytes("utf-8");

        // when
        OutputStream outputStream = exportFile.getOutputStream();
        outputStream.write(EXPECTED_CONTENT);
        outputStream.close();

        // then
        File expectedFile = new File(temporaryFolder.getRoot(), "export_82010203042A060708.csv");
        assertTrue(expectedFile.exists());
        Assert.assertArrayEquals(EXPECTED_CONTENT,IOUtils.toByteArray(new FileInputStream(expectedFile)));
    }

    @Test
    public void buildFilenameFromTaskId() throws IOException {
        ExportFile streamSetup = new ExportFile(new ExportParams(), testTaskId(),dfs);
        byte[] taskId = testTaskId();
        assertEquals("export_82010203042A060708.csv", streamSetup.buildFilenameFromTaskId(taskId));
    }

    @Test
    public void delete() throws IOException {
        // given
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportFile exportFile = new ExportFile(exportParams, testTaskId(),dfs);

        exportFile.getOutputStream();
        assertTrue("export file should exist in temp dir", temporaryFolder.getRoot().list().length > 0);

        // when
        exportFile.delete();

        // then
        assertTrue("export file should be deleted", temporaryFolder.getRoot().list().length == 0);
    }

    @Test
    public void createDirectory() throws IOException {
        String testDir = temporaryFolder.getRoot().getAbsolutePath() + "/" + RandomStringUtils.randomAlphabetic(9);
        ExportParams exportParams = ExportParams.withDirectory(testDir);
        ExportFile exportFile = new ExportFile(exportParams, testTaskId(),dfs);

        assertTrue(exportFile.createDirectory());

        assertTrue(new File(testDir).exists());
        assertTrue(new File(testDir).isDirectory());
    }

    @Test
    public void createDirectory_returnsFalseWhenCannotCreate() throws IOException {
        String testDir = "/noPermissionToCreateFolderInRoot";
        ExportParams exportParams = ExportParams.withDirectory(testDir);
        ExportFile exportFile = new ExportFile(exportParams, testTaskId(),dfs);

        assertFalse(exportFile.createDirectory());

        assertFalse(new File(testDir).exists());
        assertFalse(new File(testDir).isDirectory());
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static byte[] testTaskId() {
        return new byte[]{
                -126, 1, 2, 3, 4, 42, 6, 7, 8
        };
    }


}