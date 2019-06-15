package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedResultSetMetaData;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yxia on 4/25/19.
 */
public class SpliceGroupUserVTI implements DatasetProvider, VTICosting {
    protected OperationContext operationContext;
    public static final int INCLUDINGSELFANDPUBLIC = 1;
    public static final int ADMINONLY = 2;
    public static final int DEFAULT = 0;

    private int returnType;

    public SpliceGroupUserVTI () {
        this.returnType = DEFAULT;
    }

    public SpliceGroupUserVTI (int returnType) {
        if (returnType == ADMINONLY || returnType == INCLUDINGSELFANDPUBLIC)
            this.returnType = returnType;
        else
            this.returnType = DEFAULT;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);

        //Create an arraylist to store the list of group users
        ArrayList<ExecRow> items = new ArrayList<ExecRow>();

        Activation activation = op.getActivation();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        String currentUser = lcc.getCurrentUserId(activation);
        List<String> groupUsers = lcc.getCurrentGroupUser(activation);
        if (returnType == ADMINONLY) {
            if (currentUser.equals("SPLICE") || groupUsers!= null && groupUsers.contains("SPLICE")) {
                ValueRow valueRow = new ValueRow(1);
                valueRow.setColumn(1, new SQLVarchar("SPLICE"));
                items.add(valueRow);
            }
            return dsp.createDataSet(items.iterator());
        }

        if (returnType != DEFAULT) {
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1, new SQLVarchar(currentUser));
            items.add(valueRow);
            valueRow = new ValueRow(1);
            valueRow.setColumn(1, new SQLVarchar("PUBLIC"));
            items.add(valueRow);
        }

        if (groupUsers != null) {
            for (String user : groupUsers) {
                ValueRow valueRow = new ValueRow(1);
                valueRow.setColumn(1, new SQLVarchar(user));
                items.add(valueRow);
            }
        }

        return dsp.createDataSet(items.iterator());
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment arg0)
            throws SQLException {
        return 1;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
        return 1;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
            throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }

    /*
     * Metadata
     */
    private static final ResultColumnDescriptor[] columnInfo = {
            EmbedResultSetMetaData.getResultColumnDescriptor("USERNAME", Types.VARCHAR, false, 128)
    };

    private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);

}