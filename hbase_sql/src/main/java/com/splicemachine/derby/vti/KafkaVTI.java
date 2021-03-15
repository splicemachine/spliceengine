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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class KafkaVTI implements DatasetProvider, VTICosting{
    private static final Logger LOG = Logger.getLogger(KafkaVTI.class.getName());
    private OperationContext operationContext;
    public static final ThreadLocal<Dataset<Row>> datasetThreadLocal = new ThreadLocal();

    private String topicName;
    private String host;
    private String port;

    public KafkaVTI() {}

    public KafkaVTI(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        DataSet<ExecRow> ds;
        if (op != null)
            operationContext = dsp.createOperationContext(op);
        else
            operationContext = dsp.createOperationContext((Activation)null);
        try
        {
            operationContext.pushScopeForOp("Read Kafka Topic");
            ds = dsp.readKafkaTopic(topicName, operationContext);
        }
        finally {
            operationContext.popScope();
        }
        return ds;
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
