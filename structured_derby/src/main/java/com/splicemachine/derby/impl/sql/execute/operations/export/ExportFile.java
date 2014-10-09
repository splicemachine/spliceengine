package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Encapsulates logic about how taskId + ExportParams are translated into target file path, how file (and directory)
 * are created, etc.
 */
class ExportFile {

    private final FileSystem fileSystem;
    private final ExportParams exportParams;
    private final byte[] taskId;

    ExportFile(ExportParams exportParams, byte[] taskId) throws IOException {
        this.fileSystem = FileSystem.get(SpliceUtils.config);
        this.exportParams = exportParams;
        this.taskId = taskId;
    }

    public OutputStream getOutputStream() throws IOException {
        // Filename
        Path fullyQualifiedExportFilePath = buildOutputFilePath();

        // OutputStream
        return fileSystem.create(fullyQualifiedExportFilePath, exportParams.getReplicationCount());
    }

    // Create the directory if it doesn't exist.
    public boolean createDirectory() {
        try {
            Path directoryPath = new Path(exportParams.getDirectory());
            return fileSystem.mkdirs(directoryPath);
        } catch (IOException e) {
            return false;
        }
    }

    public void delete() throws IOException {
        fileSystem.delete(buildOutputFilePath(), false);
    }

    protected Path buildOutputFilePath() {
        Path directoryPath = new Path(exportParams.getDirectory());
        String exportFile = buildFilenameFromTaskId(taskId);
        return new Path(directoryPath, exportFile);
    }

    protected String buildFilenameFromTaskId(byte[] taskId) {
        return "export_" + BytesUtil.toHex(taskId) + ".csv";
    }

}
