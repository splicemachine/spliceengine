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
import org.apache.derby.iapi.types.*;
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
import java.lang.reflect.Field;

public class XPlainTraceTreePrinter implements XPlainTracePrinter {

    private Connection connection;
    private ExecRow dataTemplate;
    private ArrayList<ExecRow> rows;
    private static final String branch = "|--";
    private static final String trunk = "|  ";
    private static final String space = "   ";
    private SortedMap<Integer, Integer> trunks;
    private int mode;
    private XPlainTraceLegend legend;
    private XPlainTreeNode topOperation;

    public XPlainTraceTreePrinter (int mode, Connection connection, XPlainTreeNode topOperation) {
        this.mode = mode;
        this.connection = connection;
        this.dataTemplate = new ValueRow(1);
        this.dataTemplate.setRowArray(new DataValueDescriptor[]{new SQLClob()});
        this.rows = new ArrayList<ExecRow>(20);
        this.trunks = new TreeMap<Integer, Integer>();
        this.topOperation = topOperation;
        this.legend = new XPlainTraceLegend();
    }

    public ResultSet print() throws SQLException, StandardException, IllegalAccessException{

        printOperationTree(topOperation, 0);

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

    private void printOperationTree(XPlainTreeNode operation, int level)
            throws StandardException, SQLException, IllegalAccessException{
        dataTemplate.resetRowArray();
        StringBuilder sb = new StringBuilder();
        int sequenceId = operation.getSequenceId();
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
        sb.append(operation.getOperationType());

        if (mode == 1) {
            StringBuilder metrics = getMetrics(operation);
            if (metrics.toString().length() > 2) {
                sb.append(metrics);
            }
        }
        DataValueDescriptor[] dvds = dataTemplate.getRowArray();
        dvds[0].setValue(sb.toString());
        rows.add(dataTemplate.getClone());

        if (operation.getChildren().size() == 2) {
            int rightChild = operation.getChildren().getLast().getSequenceId();
            trunks.put(new Integer(level+1), rightChild);
        }

        for (XPlainTreeNode child:operation.getChildren()) {
            printOperationTree(child, level+1);
        }
    }



    private StringBuilder getMetrics(XPlainTreeNode operation) throws SQLException, IllegalAccessException {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        Field[] fields = operation.getClass().getDeclaredFields();
        for (Field field:fields) {
            field.setAccessible(true);
            String columnName = field.getName().toUpperCase();
            String val;
            if (isMetricColumn(columnName)) {
                if (isStringMetric(columnName)) {
                    val = (String)field.get(operation);
                    if (val != null) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(",");
                        }
                        legend.use(columnName);
                        sb.append(legend.getShortName(columnName)).append("=").append(val);
                    }

                }
                else {
                    long l = field.getLong(operation);
                    if (columnName.endsWith("TIME")) {
                        l = l / 1000000;
                    }
                    if (l > 0) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(",");
                        }
                        legend.use(columnName);
                        sb.append(legend.getShortName(columnName)).append("=").append(l);
                    }
                }
            }
        }
        sb.append("]");
        return sb;
    }

    private boolean isMetricColumn(String columnName) {
        return (columnName.toUpperCase().compareTo("TABLESCAN") != 0 &&
                columnName.toUpperCase().compareTo("STATEMENTID") != 0 &&
                columnName.toUpperCase().compareTo("ISRIGHTCHILDOP") != 0 &&
                columnName.toUpperCase().compareTo("SEQUENCEID") != 0 &&
                columnName.toUpperCase().compareTo("PARENTOPERATIONID") != 0 &&
                columnName.toUpperCase().compareTo("RIGHTCHILD") != 0 &&
                columnName.toUpperCase().compareTo("CHILDREN") != 0 &&
                columnName.toUpperCase().compareTo("FIELDS") != 0 &&
                columnName.toUpperCase().compareTo("FIELDMAP") != 0 &&
                columnName.toUpperCase().compareTo("OPERATIONTYPE") != 0);
    }

    private boolean isStringMetric(String columnName) {

        return (columnName.toUpperCase().compareTo("HOST") == 0 ||
                columnName.toUpperCase().compareTo("REGION") == 0);

    }
}
