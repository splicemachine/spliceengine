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

import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.derby.impl.load.ImportUtils;
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
public class ExportFile {

    public static final String SUCCESS_FILE = "_SUCCESS";
    private final DistributedFileSystem fileSystem;
    private final ExportParams exportParams;
    private final byte[] taskId;
    private static Logger LOG=Logger.getLogger(ExportFile.class);

    ExportFile(ExportParams exportParams, byte[] taskId) throws IOException {
        this(exportParams, taskId, SIDriver.driver().getFileSystem(exportParams.getDirectory()));
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
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "deleteDirectory()");
        try{
            fileSystem.delete(exportParams.getDirectory(), true);
            return true;
        }catch(NoSuchFileException fnfe){
            return false;
        }
    }

    public boolean clearDirectory() throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "clearDirectory(): dir=%s", exportParams.getDirectory());
        try{
            // Delete the empty SUCCESS file and any prior exported data files
            fileSystem.delete(exportParams.getDirectory(), SUCCESS_FILE, false);
            deletePriorExportFiles();
            return true;
        }catch(NoSuchFileException e){
            return false;
        }
    }

    private void deletePriorExportFiles() throws IOException {
        boolean isTrace = LOG.isTraceEnabled();
        String[] files = fileSystem.getExistingFiles(exportParams.getDirectory(), "part-r-*");
        for (String file : files) {
            if (isTrace)
                SpliceLogUtils.trace(LOG, "Deleting file: %s/%s", exportParams.getDirectory(), file);
            try {
                if (file != null && !file.isEmpty())
                    fileSystem.delete(exportParams.getDirectory(), file, false);
            } catch(NoSuchFileException e) {
                SpliceLogUtils.warn(LOG, "Unable to delete file %s, but that won't prevent export.");
            }
        }
    }

    public boolean isWritable(){
        try {
            ImportUtils.validateWritable(exportParams.getDirectory(),false);
            return true;
        } catch (Exception e) {
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
