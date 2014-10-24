package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.job.JobResults;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Used by ExportOperation to cleanup any partial export files left by failed tasks. The tasks attempt to delete
 * their partial files in the event of failure but their ability to do so depends on the type of failure.
 */
class ExportFailedTaskCleanup {

    private static final Logger LOG = Logger.getLogger(ExportFailedTaskCleanup.class);

    public void cleanup(JobResults jobResults, ExportParams exportParams) {
        if (jobResults != null && jobResults.getJobStats() != null && jobResults.getJobStats().getFailedTasks() != null) {
            cleanupForTask(jobResults.getJobStats().getFailedTasks(), exportParams);
        }
    }

    private void cleanupForTask(List<byte[]> failedTaskIdList, ExportParams exportParams) {
        for (byte[] failedTaskId : failedTaskIdList) {
            uncheckedDelete(exportParams, failedTaskId);
        }
    }

    private void uncheckedDelete(ExportParams exportParams, byte[] failedTaskId) {
        try {
            ExportFile exportFile = new ExportFile(exportParams, failedTaskId);
            boolean deleteResult = exportFile.delete();
            LOG.info(String.format("delete export file for failed task '%s', result='%s'", BytesUtil.toHex(failedTaskId), deleteResult));
        } catch (IOException e) {
            LOG.error("task failed and then was unable to delete export file", e);
        }
    }

}
