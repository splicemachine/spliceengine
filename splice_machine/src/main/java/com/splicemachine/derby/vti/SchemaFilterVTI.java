package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedResultSetMetaData;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
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

public class SchemaFilterVTI implements DatasetProvider, VTICosting {
    private OperationContext operationContext;

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);

        ArrayList<ExecRow> items = new ArrayList<ExecRow>();

        Activation activation = op.getActivation();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

        TransactionController tc = lcc.getTransactionExecute();
        DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
        List<SchemaDescriptor> schemas= dd.getAllSchemas(tc);

        for(SchemaDescriptor schema: schemas) {
            String schemaName = schema.getSchemaName();
            if (lcc.getAuthorizer().canSeeSchema(activation, schemaName, schema.getAuthorizationId())) {
                ValueRow valueRow = new ValueRow(1);
                valueRow.setColumn(1, new SQLVarchar(schemaName));
                items.add(valueRow);
            }
        }

        return dsp.createDataSet(items.iterator());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    private static final ResultColumnDescriptor[] columnInfo = {
            EmbedResultSetMetaData.getResultColumnDescriptor("SCHEMA_NAME", Types.VARCHAR, false, 128),
    };

    private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
}
