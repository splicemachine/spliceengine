package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;

/**
 * Encapsulates logic about how taskId + ExportParams are translated into target file path, how file (and directory)
 * are created, etc.
 */
class ExportFile {

    private final DistributedFileSystem fileSystem;
    private final ExportParams exportParams;
    private final byte[] taskId;

    ExportFile(ExportParams exportParams, byte[] taskId) throws IOException {
        this(exportParams, taskId, SIDriver.driver().fileSystem());
    }

    ExportFile(ExportParams exportParams, byte[] taskId, DistributedFileSystem fileSystem) throws IOException {
        this.exportParams = exportParams;
        this.taskId = taskId;
        this.fileSystem = fileSystem;
    }
    
    public OutputStream getOutputStream() throws IOException {
        // Filename
        Path fullyQualifiedExportFilePath = buildOutputFilePath();

        // OutputStream
        OutputStream rawOutputStream =fileSystem.newOutputStream(fullyQualifiedExportFilePath,
                new DistributedFileOpenOption(exportParams.getReplicationCount(),StandardOpenOption.CREATE_NEW));

        return exportParams.isCompression() ? new GZIPOutputStream(rawOutputStream) : rawOutputStream;
    }

    // Create the directory if it doesn't exist.
    public boolean createDirectory() {
        try {
            Path directoryPath = fileSystem.getPath(exportParams.getDirectory());
            return fileSystem.createDirectory(directoryPath,false);
        } catch (IOException e) {
            return false;
        }
    }

    public boolean delete() throws IOException {
        try{
            fileSystem.delete(buildOutputFilePath());
            return true;
        }catch(NoSuchFileException fnfe){
            return false;
        }
    }

    public boolean deleteDirectory() throws IOException {
        try{
            fileSystem.delete(fileSystem.getPath(exportParams.getDirectory()),true);
            return true;
        }catch(NoSuchFileException fnfe){
            return false;
        }
    }

    public boolean isWritable(){
        return Files.isWritable(fileSystem.getPath(exportParams.getDirectory()));
    }

    protected Path buildOutputFilePath() {
        return fileSystem.getPath(exportParams.getDirectory(),buildFilenameFromTaskId(taskId));
    }

    protected String buildFilenameFromTaskId(byte[] taskId) {
        return "export_" + Bytes.toHex(taskId) + ".csv" + (exportParams.isCompression() ? ".gz" : "");
    }

}
