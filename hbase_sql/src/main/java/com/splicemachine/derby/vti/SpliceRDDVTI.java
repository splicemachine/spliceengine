package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SpliceRDDVTI implements DatasetProvider, VTICosting {
    protected OperationContext operationContext;
    public static final ThreadLocal<JavaRDD<Row>> datasetThreadLocal = new ThreadLocal();
    public SpliceRDDVTI() {

    }

    public static DatasetProvider getSpliceRDDVTI() {
        return new SpliceRDDVTI();
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op,DataSetProcessor dsp,  ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);
        if (datasetThreadLocal.get() == null)
            throw new RuntimeException("dataset is null");
        return SparkDataSet.toSpliceLocatedRow(datasetThreadLocal.get(),operationContext);
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100000;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100000;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

}
