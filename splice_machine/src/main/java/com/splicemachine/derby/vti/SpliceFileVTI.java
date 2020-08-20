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

package com.splicemachine.derby.vti;

import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.function.StreamFileFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 *
 *
 *
 *
 */
public class SpliceFileVTI implements DatasetProvider, VTICosting {
    private String fileName;
    private String characterDelimiter;
    private String columnDelimiter;
    private String timeFormat;
    private String dateTimeFormat;
    private String timestampFormat;
    private int[] columnIndex;
    private OperationContext operationContext;
    private boolean oneLineRecords;
    private String charset;
    private String statusDirectory = null;
    private int badRecordsAllowed = 0;

    public SpliceFileVTI() {}

    public SpliceFileVTI(String fileName) {
        this.fileName = fileName;
        oneLineRecords = true;
        charset = StandardCharsets.UTF_8.name();
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter) {
        this(fileName);
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, boolean oneLineRecords) {
        this(fileName);
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
        this.oneLineRecords = oneLineRecords;
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex) {
        this(fileName, characterDelimiter, columnDelimiter);
        this.columnIndex = columnIndex;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex, String timeFormat, String dateTimeFormat, String timestampFormat) {
        this(fileName, characterDelimiter, columnDelimiter,columnIndex);
        this.timeFormat = timeFormat;
        this.dateTimeFormat = dateTimeFormat;
        this.timestampFormat = timestampFormat;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex, String timeFormat, String dateTimeFormat, String timestampFormat, String oneLineRecords, String charset) throws StandardException {
        this(fileName, characterDelimiter, columnDelimiter,columnIndex,timeFormat,dateTimeFormat,timestampFormat);
        this.oneLineRecords = Boolean.parseBoolean(oneLineRecords);
        this.charset = charset;
        if (!Charset.isSupported(charset))
            throw StandardException.newException(SQLState.UNSUPPORTED_ENCODING_EXCEPTION,charset);
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex, String timeFormat, String dateTimeFormat, String timestampFormat, String oneLineRecords, String charset, String statusDirectory, int badRecordsAllowed) throws StandardException {
        this(fileName, characterDelimiter, columnDelimiter,columnIndex,timeFormat,dateTimeFormat,timestampFormat,oneLineRecords,charset);
        this.statusDirectory = statusDirectory;
        this.badRecordsAllowed = badRecordsAllowed;
    }

    public static DatasetProvider getSpliceFileVTI(String fileName) {
        return new SpliceFileVTI(fileName);
    }

    public static DatasetProvider getSpliceFileVTI(String fileName, String characterDelimiter, String columnDelimiter) {
        return new SpliceFileVTI(fileName,characterDelimiter,columnDelimiter);
    }

    public static DatasetProvider getSpliceFileVTI(String fileName, String characterDelimiter, String columnDelimiter, int[] columnIndex) {
        return new SpliceFileVTI(fileName,characterDelimiter,columnDelimiter, columnIndex);
    }

    public static DatasetProvider getSpliceFileVTI(String fileName, String characterDelimiter, String columnDelimiter, int[] columnIndex,
                                                   String timeFormat, String dateTimeFormat, String timestampFormat) {
        return new SpliceFileVTI(fileName,characterDelimiter,columnDelimiter, columnIndex,timeFormat,dateTimeFormat,timestampFormat);
    }


    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        if (op != null)
            operationContext = dsp.createOperationContext(op);
        else // this call works even if activation is null
            operationContext = dsp.createOperationContext((Activation)null);
        try {
            ImportUtils.validateReadable(fileName, false);
            if (statusDirectory != null)
                operationContext.setPermissive(statusDirectory, fileName, badRecordsAllowed);
            if (oneLineRecords && (charset==null || charset.toLowerCase().equals("utf-8"))) {
                DataSet<String> textSet = dsp.readTextFile(fileName, op);
                operationContext.pushScopeForOp("Parse File");
                return textSet.flatMap(new FileFunction(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat, dateTimeFormat, timestampFormat, true, operationContext), true);
            } else {
                PairDataSet<String,InputStream> streamSet = dsp.readWholeTextFile(fileName, op);
                operationContext.pushScopeForOp("Parse File");
                return streamSet.values(operationContext).flatMap(new StreamFileFunction(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat, dateTimeFormat, timestampFormat, charset, operationContext), true);
            }
        } finally {
            operationContext.popScope();
        }
    }

    private static final int defaultBytesPerRow = 100;
    public static int getBytesPerRow() {
        // Imprecise assumption of a fixed number of bytes per row,
        // but better than assuming fixed default row count.
        // Future improvements might include:
        // 1) fetch first few rows and compute real bytes per row and extrapolate
        // 2) count number of columns and assume bytes per column
        // In the end, though, this will *always* be just an approximation,
        // because we can't actually count the rows before importing.
        return defaultBytesPerRow;
    }

    private FileInfo fileInfo = null;
    protected FileInfo getFileInfo() throws StandardException, IOException {
        if (fileInfo == null) {
            if (fileName != null) fileInfo = ImportUtils.getImportFileInfo(fileName);
        }
        return fileInfo;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        FileInfo fileInfo;
        try {
            fileInfo = getFileInfo();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        if (fileInfo != null &&fileInfo.exists()) {
            assert getBytesPerRow() != 0;
            return fileInfo.size()/ (double)getBytesPerRow();
        }
        return VTICosting.defaultEstimatedRowCount;
    }


    // Like the above row count estimate, our cost estimate is an approximation
    // using fixed assumptions, but much better than just returning same
    // default value all the time. We assume a disk throughput and
    // network throughput and derive an approximate latency.
    //
    // Disk throughput = 20 MB/S
    // Network throughput = 144MB/S (1GE card)
    // Disk latency for 1 MB = 1/throughput = 1/20 = 0.05
    // Network latency = 1/144 = 0.007
    // Total latency = 0.05 + 0.007 = 0.057 S/MB = 57000 micros/MB
    // File size (MB) = 724 MB (size of tpch1g lineitem file)
    // Total time (cost) = total latency * file size (MB) = 57000 micros/MB * 724 = 41268000 micros

    public static final double defaultDiskThroughputMBPerSecond = 20d;
    public static final double defaultNetworkThroughputMBPerSecond = 144d;
    public static final double defaultTotalLatencyMicrosPerMB =
        1000000d * /* seconds to microseconds */
        ((1 / defaultDiskThroughputMBPerSecond) /* disk latency for 1 MB */ +
         (1 / defaultNetworkThroughputMBPerSecond)) /* network latency for 1 MB */;


    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        FileInfo fileInfo;
        try {
            fileInfo = getFileInfo();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        if (fileInfo != null && fileInfo.exists()) {
            // IMPORTANT: this method needs to return MICROSECONDS, the internal unit
            // for costs in splice machine (displayed as milliseconds in explain plan output).
            // Note that size() is in bytes.
            return defaultTotalLatencyMicrosPerMB *fileInfo.size() /* bytes */ / 1000000d;
        }
        return VTICosting.defaultEstimatedCost;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("not supported");
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    public String getFileName() {
        return fileName;
    }
}
