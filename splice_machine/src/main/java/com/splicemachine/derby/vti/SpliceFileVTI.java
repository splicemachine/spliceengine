package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by jleach on 10/7/15.
 */
public class SpliceFileVTI implements DatasetProvider, VTICosting {
    private String fileName;
    private String characterDelimiter;
    private String columnDelimiter;
    private int numberOfColumns;
    public SpliceFileVTI() {

    }
    public SpliceFileVTI(String fileName) {
        this.fileName = fileName;
    }
    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter) {
        this.fileName = fileName;
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
    }

    public SpliceFileVTI(String fileName,String characterDelimiter, String columnDelimiter, int numberOfColumns) {
        this.fileName = fileName;
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
        this.numberOfColumns = numberOfColumns;
    }


    public static DatasetProvider getSpliceFileVTI(String fileName) {
        return new SpliceFileVTI(fileName);
    }

    public static DatasetProvider getSpliceFileVTI(String fileName, String characterDelimiter, String columnDelimiter) {
        return new SpliceFileVTI(fileName,characterDelimiter,columnDelimiter);
    }

    public static DatasetProvider getSpliceFileVTI(String fileName, String characterDelimiter, String columnDelimiter, int numberOfColumns) {
        return new SpliceFileVTI(fileName,characterDelimiter,columnDelimiter,numberOfColumns);
    }

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
       return dsp.readTextFile(fileName).map(new FileFunction(characterDelimiter,columnDelimiter,execRow));
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1000;
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
        // Non-Table Function Definition of File...



        throw new SQLException("not supported");
    }

    private class FileResultSetMetaData implements ResultSetMetaData {
        @Override
        public int getColumnCount() throws SQLException {
            return numberOfColumns;
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isCaseSensitive(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isSearchable(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isCurrency(int column) throws SQLException {
            return false;
        }

        @Override
        public int isNullable(int column) throws SQLException {
            return 1;
        }

        @Override
        public boolean isSigned(int column) throws SQLException {
            return false;
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException {
            return 0;
        }

        @Override
        public String getColumnLabel(int column) throws SQLException {
            return null;
        }

        @Override
        public String getColumnName(int column) throws SQLException {
            return null;
        }

        @Override
        public String getSchemaName(int column) throws SQLException {
            return null;
        }

        @Override
        public int getPrecision(int column) throws SQLException {
            return 0;
        }

        @Override
        public int getScale(int column) throws SQLException {
            return 0;
        }

        @Override
        public String getTableName(int column) throws SQLException {
            return null;
        }

        @Override
        public String getCatalogName(int column) throws SQLException {
            return null;
        }

        @Override
        public int getColumnType(int column) throws SQLException {
            return 0;
        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
            return null;
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isWritable(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException {
            return false;
        }

        @Override
        public String getColumnClassName(int column) throws SQLException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }

}
