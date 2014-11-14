package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.threadpool.ThreadPoolStatus;
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
import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.util.Pair;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public class PipelineAdmin extends BaseAdminProcedures{
    private static final ResultColumnDescriptor[] WRITE_INTAKE_COLUMNS = new ResultColumnDescriptor[]{
            new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("totalWriteThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("activeWriteThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("availableIndependentPermits", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("availableDependentPermits", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("avgThroughput",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("oneMinAvgThroughput",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("fiveMinAvgThroughput",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("fifteenMinAvgThroughput",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("totalRejected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };
    public static void SYSCS_GET_WRITE_INTAKE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new BaseAdminProcedures.JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<SpliceIndexEndpoint.ActiveWriteHandlersIface> writeHandlers = JMXUtils.getActiveWriteHandlers(connections);
                ExecRow template = buildExecRow(WRITE_INTAKE_COLUMNS);
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(writeHandlers.size());
                int i=0;
                for (SpliceIndexEndpoint.ActiveWriteHandlersIface writeHandler : writeHandlers) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(writeHandler.getTotalWriteThreads());
                        dvds[2].setValue(writeHandler.getOccupiedWriteThreads());
                        dvds[3].setValue(writeHandler.getAvailableIndependentPermits());
                        dvds[4].setValue(writeHandler.getAvailableDependentPermits());
                        dvds[5].setValue(writeHandler.getOverallAvgThroughput());
                        dvds[6].setValue(writeHandler.get1MThroughput());
                        dvds[7].setValue(writeHandler.get5MThroughput());
                        dvds[8].setValue(writeHandler.get15MThroughput());
                        dvds[9].setValue(writeHandler.getTotalRejected());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    rows.add(template.getClone());
                    i++;
                }

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, WRITE_INTAKE_COLUMNS,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

                resultSet[0] = ers;
            }
        });
    }

    private static final ResultColumnDescriptor[] WRITE_PIPELINE_COLUMNS = new ResultColumnDescriptor[]{
            new GenericColumnDescriptor("host", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("maxIntakeThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("activeIntakeThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("maxOutputThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("activeOutputThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("rejectedIntakeWriteRequests",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("rejectedOutputWriteRequests",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("avgIntakeThroughput",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE))
    };
    public static void SYSCS_GET_WRITE_PIPELINE_INFO(final ResultSet[] resultSet) throws SQLException{
        final Map<String,ExecRow> hostRows = Maps.newHashMap();
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(jmxConnector);
                ExecRow template = buildExecRow(WRITE_PIPELINE_COLUMNS);
                int i=0;
                for(ThreadPoolStatus threadPool:threadPools){
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    String host = jmxConnector.get(i).getFirst();
                    try{
                        dvds[0].setValue(host);
                        dvds[3].setValue(threadPool.getMaxThreadCount());
                        dvds[4].setValue(threadPool.getActiveThreadCount());
                        dvds[6].setValue(threadPool.getTotalRejectedTasks());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    hostRows.put(host,template.getClone());
                }
            }
        });

        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException {
                List<SpliceIndexEndpoint.ActiveWriteHandlersIface> activeWriteHandlers = JMXUtils.getActiveWriteHandlers(jmxConnector);
                int i=0;
                for(SpliceIndexEndpoint.ActiveWriteHandlersIface writeHandler:activeWriteHandlers){
                    String host = jmxConnector.get(i).getFirst();
                    ExecRow row = hostRows.get(host);
                    if(row==null)
                        row = buildExecRow(WRITE_PIPELINE_COLUMNS);
                    DataValueDescriptor[] dvds = row.getRowArray();
                    try{
                        dvds[1].setValue(writeHandler.getTotalWriteThreads());
                        dvds[2].setValue(writeHandler.getOccupiedWriteThreads());
                        dvds[5].setValue(writeHandler.getTotalRejected());
                        dvds[7].setValue(writeHandler.getOverallAvgThroughput());
                    } catch (StandardException e) {
                        throw PublicAPI.wrapStandardException(e);
                    }
                }
            }
        });

        List<ExecRow> rows = Lists.newArrayList(hostRows.values());
        EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, WRITE_PIPELINE_COLUMNS,lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

        resultSet[0] = ers;
    }

    private static final ResultColumnDescriptor []WRITE_OUTPUT_COLUMNS = new ResultColumnDescriptor[]{
            new GenericColumnDescriptor("host", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("maxThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("activeThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("pendingWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("totalSubmittedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("totalSuccessfulWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("totalFailedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("totalRejectedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    public static void SYSCS_GET_WRITE_OUTPUT_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
                ExecRow template = buildExecRow(WRITE_OUTPUT_COLUMNS);
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(threadPools.size());
                int i=0;
                for (ThreadPoolStatus threadPool : threadPools) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(threadPool.getMaxThreadCount());
                        dvds[2].setValue(threadPool.getActiveThreadCount());
                        dvds[3].setValue(threadPool.getPendingTaskCount());
                        dvds[4].setValue(threadPool.getTotalSubmittedTasks());
                        dvds[5].setValue(threadPool.getTotalSuccessfulTasks());
                        dvds[6].setValue(threadPool.getTotalFailedTasks());
                        dvds[7].setValue(threadPool.getTotalRejectedTasks());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    rows.add(template.getClone());
                    i++;
                }

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, WRITE_OUTPUT_COLUMNS,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

                resultSet[0] = ers;
            }
        });
    }
}
