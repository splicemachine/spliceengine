package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.*;
import java.util.zip.GZIPOutputStream;

/**
 * Encapsulates logic about how taskId + ExportParams are translated into target file path, how file (and directory)
 * are created, etc.
 */
class ExportFile {

    private final DistributedFileSystem fileSystem;
    private final ExportParams exportParams;
    private final byte[] taskId;
    private static Logger LOG=Logger.getLogger(ExportFile.class);

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
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createDirectory(): export directory=%s", exportParams.getDirectory());
        try {
            return fileSystem.createDirectory(exportParams.getDirectory(),false);
        } catch (IOException e) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "createDirectory(): exception trying to create directory %s: %s", exportParams.getDirectory(), e);
            return false;
        }
    }

    public boolean delete() throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "delete()");
        try{
            fileSystem.delete(exportParams.getDirectory(),buildFilenameFromTaskId(taskId), false);
            return true;
        }catch(NoSuchFileException fnfe){
            return false;
        }
    }

    public boolean deleteDirectory() throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "deleteDirectory()");
        try{
            fileSystem.delete(exportParams.getDirectory(), true);
            return true;
        }catch(NoSuchFileException fnfe){
            return false;
        }
    }

    public boolean isWritable(){
        try {
            FileInfo info = fileSystem.getInfo(exportParams.getDirectory());
            return info != null && info.isWritable();
        } catch (IOException e) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "isWritable(): exception trying to check writable path %s: %s", exportParams.getDirectory(), e);
            return false;
        }
    }

    protected Path buildOutputFilePath() {
        return fileSystem.getPath(exportParams.getDirectory(),buildFilenameFromTaskId(taskId));
    }

    protected String buildFilenameFromTaskId(byte[] taskId) {
        return "export_" + Bytes.toHex(taskId) + ".csv" + (exportParams.isCompression() ? ".gz" : "");
    }

}
