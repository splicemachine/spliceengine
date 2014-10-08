package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.derby.iapi.error.StandardException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.*;

public class ExportFileTest {


    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void getOutputStream_createsStreamConnectedToExpectedFile() throws IOException {

        // given
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportFile exportFile = new ExportFile(exportParams, testTaskId());
        final byte[] EXPECTED_CONTENT = ("splice for the win" + RandomStringUtils.randomAlphanumeric(1000)).getBytes("utf-8");

        // when
        OutputStream outputStream = exportFile.getOutputStream();
        outputStream.write(EXPECTED_CONTENT);
        outputStream.close();

        // then
        File expectedFile = new File(temporaryFolder.getRoot(), "export_82010203042A060708.csv");
        assertTrue(expectedFile.exists());
        assertArrayEquals(EXPECTED_CONTENT, IOUtils.toByteArray(new FileInputStream(expectedFile)));
    }

    @Test
    public void buildFilenameFromTaskId() throws IOException {
        ExportFile streamSetup = new ExportFile(new ExportParams(), testTaskId());
        byte[] taskId = testTaskId();
        assertEquals("export_82010203042A060708.csv", streamSetup.buildFilenameFromTaskId(taskId));
    }

    @Test
    public void delete() throws IOException {
        // given
        ExportParams exportParams = ExportParams.withDirectory(temporaryFolder.getRoot().getAbsolutePath());
        ExportFile exportFile = new ExportFile(exportParams, testTaskId());

        exportFile.getOutputStream();
        assertTrue("export file should exist in temp dir", temporaryFolder.getRoot().list().length > 0);

        // when
        exportFile.delete();

        // then
        assertTrue("export file should be deleted", temporaryFolder.getRoot().list().length == 0);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static byte[] testTaskId() {
        return new byte[]{
                -126, 1, 2, 3, 4, 42, 6, 7, 8
        };
    }


}