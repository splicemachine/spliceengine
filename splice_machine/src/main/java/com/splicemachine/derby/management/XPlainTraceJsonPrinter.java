package com.splicemachine.derby.management;

import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLClob;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * Created by jyuan on 5/15/14.
 */
public class XPlainTraceJsonPrinter extends XPlainTraceBasePrinter{

    private XPlainTreeNode topOperation;
    private Connection connection;
    private ArrayList<ExecRow> rows;
    private ExecRow dataTemplate;
    private int mode;

    public XPlainTraceJsonPrinter (int mode, Connection connection, XPlainTreeNode topOperation) {
        this.connection = connection;
        this.topOperation = topOperation;
        this.dataTemplate = new ValueRow(1);
        this.dataTemplate.setRowArray(new DataValueDescriptor[]{new SQLClob()});
        this.rows = new ArrayList<ExecRow>(1);
        this.mode = mode;
    }

    public ResultSet print() throws SQLException, StandardException, IllegalAccessException {

        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        String xplain = gson.toJson(topOperation);

        DataValueDescriptor[] dvds = dataTemplate.getRowArray();
        dvds[0].setValue(xplain);
        rows.add(dataTemplate.getClone());

        ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[1];
        columnInfo[0] = new GenericColumnDescriptor("PLAN", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));

        EmbedConnection defaultConn = (EmbedConnection)connection;
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }

        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);
        return ers;
    }
}
