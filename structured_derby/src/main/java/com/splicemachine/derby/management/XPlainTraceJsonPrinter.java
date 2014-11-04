package com.splicemachine.derby.management;

import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLClob;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;

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
