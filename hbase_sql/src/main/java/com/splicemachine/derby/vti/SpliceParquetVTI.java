/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

import java.sql.*;

/**
 * Created by jleach on 10/7/15.
 */
public class SpliceParquetVTI implements DatasetProvider, VTICosting {
    private String connectionUrl;
    private String schemaName;
    private String tableName;
    private String sql; // Bind Variables?
    protected OperationContext operationContext;
    public SpliceParquetVTI() {

    }
    public SpliceParquetVTI(String connectionUrl, String schemaName, String tableName) {
        this.connectionUrl = connectionUrl;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public SpliceParquetVTI(String connectionUrl, String sql) {
        this.connectionUrl = connectionUrl;
        this.sql = sql;
    }


    public static DatasetProvider getJDBCTableVTI(String connectionUrl, String schemaName, String tableName) {
        return new SpliceParquetVTI(connectionUrl, schemaName, tableName);
    }

    public static DatasetProvider getJDBCSQLVTI(String connectionUrl, String sql) {
        return new SpliceParquetVTI(connectionUrl, sql);
    }

    @Override
    public DataSet<LocatedRow> getDataSet(SpliceOperation op,DataSetProcessor dsp,  ExecRow execRow) throws StandardException {
        Connection connection = null;
        operationContext = dsp.createOperationContext(op);
        final PreparedStatement ps;
        try {
            connection = DriverManager.getConnection(connectionUrl);
            ps = connection.prepareStatement(sql != null ? sql : "select * from " + schemaName + "." + tableName);
            ResultSetIterator it = new ResultSetIterator(connection,ps,execRow);
            op.registerCloseable(it);
            return dsp.createDataSet(it);
        } catch (SQLException e) {
            throw StandardException.plainWrapException(e);
        }
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
        Connection connection = DriverManager.getConnection(connectionUrl);
        PreparedStatement ps;
        try {
            ps = connection.prepareStatement(sql != null ? sql : "select * from " + schemaName + "." + tableName);
            return ps.getMetaData();
        } finally {
            if (connection!=null)
                connection.close();
        }
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }
}
