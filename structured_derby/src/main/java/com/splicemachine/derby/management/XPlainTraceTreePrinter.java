package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/12/14.
 */

import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.IteratorNoPutResultSet;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;

import java.sql.*;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

public class XPlainTraceTreePrinter implements XPlainTracePrinter {

    private String schemaName;
    private Connection connection;
    private ExecRow dataTemplate;
    private ArrayList<ExecRow> rows;
    private static final String branch = "|--";
    private static final String trunk = "|  ";
    private static final String space = "   ";
    private SortedMap<Integer, Integer> trunks;
    private ArrayList<String> columnNames;
    private int mode;
    private XPlainTraceLegend legend;

    public XPlainTraceTreePrinter (int mode, String schemaName, Connection connection, ArrayList<String> columnNames) {
        this.mode = mode;
        this.schemaName = schemaName;
        this.connection = connection;
        this.dataTemplate = new ValueRow(1);
        this.dataTemplate.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
        this.rows = new ArrayList<ExecRow>(20);
        this.trunks = new TreeMap<Integer, Integer>();
        this.columnNames = columnNames;
        this.legend = new XPlainTraceLegend();
    }

    public ResultSet print() throws SQLException, StandardException{
        ResultSet rs = getXPlainTrace(connection);
        while (rs.next()) {
            getNextRow(rs);
        }
        rs.close();

        if (mode == 1) {
            dataTemplate.resetRowArray();
            StringBuilder sb = new StringBuilder();
            for (int i= 0; i < 80; ++i) {
                sb.append("-");
            }
            DataValueDescriptor[] dvds = dataTemplate.getRowArray();
            dvds[0].setValue(sb.toString());
            rows.add(dataTemplate.getClone());
            legend.print(rows, dataTemplate);
        }

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

    private void getNextRow(ResultSet rs) throws SQLException, StandardException{
        dataTemplate.resetRowArray();
        StringBuilder sb = new StringBuilder();
        int index = rs.findColumn("LEVEL");
        int level = rs.getInt(index);

        index = rs.findColumn("SEQUENCEID");
        int sequenceId = rs.getInt(index);

        Set<Integer> levels = trunks.keySet();
        Iterator<Integer> l = levels.iterator();
        int nextLevel = l.hasNext() ? l.next() : -1;

        for (int i = 0; i <= level; ++i) {
            if (i < nextLevel) {
                sb.append(space);
            } else if (i == nextLevel) {
                int rightChild = trunks.get(nextLevel);
                if (i == level) {
                    sb.append(branch);
                } else {
                    sb.append(trunk);
                }
                if (rightChild == sequenceId) {
                    trunks.remove(nextLevel);
                }

                if (l.hasNext()) nextLevel = l.next();
            }
            else {
                if (l.hasNext()) {
                    nextLevel = l.next();
                } else {
                    if (i == level) {
                        sb.append(branch);
                    } else {
                        sb.append(space);
                    }
                }
            }
        }
        index = rs.findColumn("OPERATIONTYPE");
        String operationType = rs.getString(index);
        sb.append(operationType);
        if (mode == 1) {
            StringBuilder metrics = getMetrics(rs);
            if (metrics.toString().length() > 2) {
                sb.append(metrics);
            }
        }
        DataValueDescriptor[] dvds = dataTemplate.getRowArray();
        dvds[0].setValue(sb.toString());
        rows.add(dataTemplate.getClone());

        index = rs.findColumn("RIGHTCHILD");
        int rightChild = rs.getInt(index);
        if (rightChild > 0) {
            trunks.put(new Integer(level+1), new Integer(rightChild));
        }
    }

    private StringBuilder getMetrics(ResultSet rs) throws SQLException{
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (String columnName:columnNames) {
            if (isMetricColumn(columnName)) {
                int index = rs.findColumn(columnName);
                long val = rs.getLong(index);
                if(columnName.endsWith("TIME")) {
                    val = val / 1000000;
                }
                if (val > 0) {
                    if (first) {
                        first = false;
                    }
                    else {
                        sb.append(",");
                    }
                    legend.use(columnName);
                    sb.append(legend.getShortName(columnName)).append("=").append(val);
                }
            }
        }
        sb.append("]");
        return sb;
    }
    private ResultSet getXPlainTrace(Connection connection) throws SQLException{
        String query = "select * from " + schemaName + ".SYSXPLAIN_TRACE order by sequenceId";
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(query.toString());
        return rs;
    }

    private boolean isMetricColumn(String columnName) {
        return (columnName.toUpperCase().compareTo("SEQUENCEID") != 0 &&
                columnName.toUpperCase().compareTo("STATEMENTID") != 0 &&
                columnName.toUpperCase().compareTo("LEVEL") != 0 &&
                columnName.toUpperCase().compareTo("RIGHTCHILD") != 0 &&
                columnName.toUpperCase().compareTo("OPERATIONTYPE") != 0 &&
                columnName.toUpperCase().compareTo("HOST") != 0 &&
                columnName.toUpperCase().compareTo("REGION") != 0);
    }
}
