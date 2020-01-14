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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.NativeSparkDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.sql.*;

/**
 * Created by jleach on 10/7/15.
 */
public class SpliceDatasetVTI implements DatasetProvider, VTICosting {
    protected OperationContext operationContext;
    public static final ThreadLocal<Dataset<Row>> datasetThreadLocal = new ThreadLocal();
    public SpliceDatasetVTI() {

    }

    public static DatasetProvider getSpliceDatasetVTI() {
        return new SpliceDatasetVTI();
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op,DataSetProcessor dsp,  ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);
        if (datasetThreadLocal.get() == null)
            throw new RuntimeException("dataset is null");
       return new NativeSparkDataSet<>(datasetThreadLocal.get(), operationContext);
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
    public ResultSetMetaData getMetaData() throws SQLException {
throw new UnsupportedOperationException();
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

}
