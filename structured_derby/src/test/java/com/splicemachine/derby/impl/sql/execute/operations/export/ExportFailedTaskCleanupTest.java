package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.google.common.collect.Lists;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ExportFailedTaskCleanupTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    @Test
    public void cleanup() throws StandardException, IOException {

        File expectedFile = new File(temporaryFolder.getRoot(), "export_82010203042A060708.csv");
        byte[] failedTaskId = new byte[]{-126, 1, 2, 3, 4, 42, 6, 7, 8};
        List<byte[]> failedTaskIdList = Lists.newArrayList(failedTaskId);

        //
        // Given: export params and a export file that already exists.
        //
        String exportPath = temporaryFolder.getRoot().getAbsolutePath();
        ExportParams exportParams = new ExportParams(exportPath, false, 1, null, null, null);
        ExportFile exportFile = new ExportFile(exportParams, failedTaskId);
        OutputStream outputStream = exportFile.getOutputStream();
        outputStream.write(1);
        outputStream.close();
        assertTrue(expectedFile.exists());

        //
        // Given: JobResults that know of the failed task
        //
        JobStats jobStats = Mockito.mock(JobStats.class);
        JobResults jobResults = Mockito.mock(JobResults.class);
        when(jobResults.getJobStats()).thenReturn(jobStats);
        when(jobStats.getFailedTasks()).thenReturn(failedTaskIdList);


        //
        // When: we call clean
        //
        new ExportFailedTaskCleanup().cleanup(jobResults, exportParams);


        //
        // Then assert the export file is deleted.
        //
        assertFalse(expectedFile.exists());

    }


}