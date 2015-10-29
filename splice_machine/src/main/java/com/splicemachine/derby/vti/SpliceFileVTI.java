package com.splicemachine.derby.vti;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by jleach on 10/7/15.
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
    public SpliceFileVTI() {

    }
    public SpliceFileVTI(String fileName) {
        this.fileName = fileName;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter) {
        this(fileName);
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex) {
        this(fileName, characterDelimiter, columnDelimiter);
        this.columnIndex = columnIndex;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int[] columnIndex, String timeFormat, String dateTimeFormat, String timestampFormat) {
        this(fileName, characterDelimiter, columnDelimiter);
        this.columnIndex = columnIndex;
        this.timeFormat = timeFormat;
        this.dateTimeFormat = dateTimeFormat;
        this.timestampFormat = timestampFormat;
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
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
            operationContext = dsp.createOperationContext(op);
        try {
            ImportUtils.validateReadable(new Path(fileName), FileSystem.get(SpliceConstants.config),false);
            DataSet<String> textSet = dsp.readTextFile(fileName);
            operationContext.pushScope(fileName);
            return textSet.flatMap(new FileFunction(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat, dateTimeFormat, timestampFormat,operationContext));
        } catch (IOException ioe) {
            throw StandardException.plainWrapException(ioe);
        }
        finally {
            operationContext.popScope();
        }
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 10000000;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 0;
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
