package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskSchedulerManagement;
import com.splicemachine.derby.impl.job.scheduler.TieredSchedulerManagement;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.logging.Logging;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.threadpool.ThreadPoolStatus;
/**
 * @author Jeff Cunningham
 *
 *         Date: 12/9/13
 */
public class SpliceAdmin extends BaseAdminProcedures {
	protected static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static Logger LOG = Logger.getLogger(SpliceAdmin.class);

    public static void SYSCS_SET_LOGGER_LEVEL(final String loggerName, final String logLevel) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    logger.setLoggerLevel(loggerName, logLevel);
                }
            }
        });
    }

    public static void SYSCS_GET_LOGGER_LEVEL(final String loggerName, final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                StringBuilder sb = new StringBuilder("select * from (values ");
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    logger.getLoggerLevel(loggerName);
                    sb.append(String.format("('%s')",
                            logger.getLoggerLevel(loggerName)));
                }
                sb.append(") foo (logLevel)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_LOGGERS(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                Set<String> uniqueLoggerNames = new HashSet<String>();
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    uniqueLoggerNames.addAll(logger.getLoggerNames());
                }
                StringBuilder sb = new StringBuilder("select * from (values ");
                List<String> loggerNames = new ArrayList<String>(uniqueLoggerNames);
                Collections.sort(loggerNames);
                for (String logger : loggerNames) {
                    sb.append(String.format("('%s')", logger));
                    sb.append(", ");
                }
                if (sb.charAt(sb.length()-2) == ',') {
                    sb.setLength(sb.length()-2);
                }
                sb.append(") foo (spliceLogger)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_ACTIVE_SERVERS(ResultSet[] resultSet) throws SQLException {
        StringBuilder sb = new StringBuilder("select * from (values ");
        int i = 0;
        for (ServerName serverName : SpliceUtils.getServers()) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(String.format("('%s',%d,%d)",
                    serverName.getHostname(),
                    serverName.getPort(),
                    serverName.getStartcode()));
            i++;
        }
        sb.append(") foo (hostname, port, startcode)");
        resultSet[0] = executeStatement(sb);
    }

    public static void SYSCS_GET_VERSION_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<SpliceMachineVersion> spliceMachineVersions = JMXUtils.getSpliceMachineVersion(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (SpliceMachineVersion spliceMachineVersion : spliceMachineVersions) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s','%s','%s','%s','%s')", connections.get(i).getFirst(),
                            spliceMachineVersion.getRelease(),
                            spliceMachineVersion.getImplementationVersion(),
                            spliceMachineVersion.getBuildTime(),
                            spliceMachineVersion.getURL()));
                    i++;
                }
                sb.append(") foo (hostname, release, implementationVersion, buildTime, url)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_SET_MAX_TASKS(final int workerTier, final int maxWorkers) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<StealableTaskSchedulerManagement> taskSchedulers = JMXUtils.getTieredSchedulerManagement(workerTier, connections);
                for (StealableTaskSchedulerManagement taskScheduler : taskSchedulers) {
                    taskScheduler.setCurrentWorkers(maxWorkers);
                }
            }
        });
    }

    // Please leave here commented out for now. Will remove this code permanently
    // once we have another mechanism by which to quickly count this from
    // a client app. For internal debugging purposes only.
    /*
    public static void SYSCS_GET_ACTIVE_JOB_COUNT(final ResultSet[] resultSet) throws SQLException {
		ResultSetBuilder rsBuilder = new ResultSetBuilder();
		try {
			rsBuilder.getColumnBuilder()
				.addColumn("JOB_ID", Types.VARCHAR, 128);

			long[] activeOperations = SpliceDriver.driver().getJobScheduler().getActiveOperations();

			RowBuilder rowBuilder = rsBuilder.getRowBuilder();
			if (activeOperations != null && activeOperations.length > 0) {
				for (int i = 0; i < activeOperations.length; i++) {
					rowBuilder.getDvd(0).setValue(activeOperations[i]);
					rowBuilder.addRow();
				}
			}
			
			resultSet[0] = rsBuilder.buildResultSet((EmbedConnection)getDefaultConn());
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		} catch (ExecutionException ee) {
			throw PublicAPI.wrapStandardException(Exceptions.parseException(ee));
		}
    }
    */

    /**
     * @deprecated desupported as of version 1.00. Throws exception if invoked.
     */
    public static void SYSCS_GET_ACTIVE_JOB_IDS(final ResultSet[] resultSet) throws SQLException {
        // This stored procedure was a temporary internal debugging mechanism
        // that was useful at the time, but it's time to purge it, because:
        // - it has not been close to working properly for a long time
        // - it's non trivial to make it work properly (all jobs/tasks in all states)
        // - it's not presenting information we need to expose via stored proc anyway
        // - SYSCS_GET_STATEMENT_SUMMARY or SYSCS_GET_REGION_SERVER_TASK_INFO
        //   are the commonly used equivalents
        //
        // Right now an upgrade from before 1.00 will not automatically delete
        // stored procedures, even if you invoke SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES().
        // Therefore, we keep this backend method but throw runtime exception,
        // in case the data dictionary still has this procedure.
        throw new SQLException(
                "The procedure SYSCS_GET_ACTIVE_JOB_IDS has been permanently desupported. Use SYSCS_GET_STATEMENT_SUMMARY or SYSCS_GET_REGION_SERVER_TASK_INFO.");
    }

    public static void SYSCS_GET_PAST_STATEMENT_SUMMARY(final ResultSet[] resultSets) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<Pair<String, StatementManagement>> statementManagers = JMXUtils.getStatementManagers(connections);

                ExecRow dataTemplate = new ValueRow(16);
                dataTemplate.setRowArray(new DataValueDescriptor[]{
                        new SQLLongint(),new SQLVarchar(),new SQLVarchar(),new SQLVarchar(),new SQLVarchar(),new SQLVarchar(),
                        new SQLInteger(),new SQLInteger(),new SQLInteger(),new SQLInteger(),
                        new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLDouble()
                });
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(statementManagers.size());
                for (Pair<String, StatementManagement> managementPair : statementManagers) {
                    StatementManagement management = managementPair.getSecond();
                    List<StatementInfo> completedStatements = management.getRecentCompletedStatements();
                    for (StatementInfo completedStatement : completedStatements) {
                        Set<JobInfo> completedJobs = completedStatement.getCompletedJobs();
                        int numFailedJobs = 0;
                        long maxJobTime = 0;
                        long minJobTime = Long.MAX_VALUE;
                        double avgJobTime = 0;
                        int numCancelledJobs = 0;
                        int count = 0;
                        if (completedJobs != null) {
                            for (JobInfo info : completedJobs) {
                                count++;
                                if (info.getJobState() == JobInfo.JobState.FAILED) {
                                    numFailedJobs++;
                                } else if (info.getJobState() == JobInfo.JobState.CANCELLED) {
                                    numCancelledJobs++;
                                } else {
                                    long jobStart = info.getJobStartMs();
                                    long jobFinish = info.getJobFinishMs();
                                    long jobTimeTaken = jobFinish - jobStart;
                                    if (maxJobTime < jobTimeTaken)
                                        maxJobTime = jobTimeTaken;
                                    if (minJobTime > jobTimeTaken)
                                        minJobTime = jobTimeTaken;
                                    avgJobTime += avgJobTime + (jobTimeTaken - avgJobTime) / count;
                                }
                            }
                        }
                        if (minJobTime == Long.MAX_VALUE)
                            minJobTime = 0;
                        int numJobs = completedJobs != null ? completedJobs.size() : 0;
                        int successfulJobs = numJobs - numFailedJobs - numCancelledJobs;
                        long startTimeMs = completedStatement.getStartTimeMs();
                        long stopTimeMs = completedStatement.getStopTimeMs();
                        dataTemplate.resetRowArray();
                        DataValueDescriptor[] dvds = dataTemplate.getRowArray();
                        try {
                            dvds[0].setValue(completedStatement.getStatementUuid());
                            dvds[1].setValue(managementPair.getFirst());
                            dvds[2].setValue(completedStatement.getUser());
                            dvds[3].setValue(completedStatement.getTxnId());
                            dvds[4].setValue(numFailedJobs>0?"FAILED" : numCancelledJobs>0? "CANCELLED":"SUCCESS");
                            dvds[5].setValue(completedStatement.getSql());
                            dvds[6].setValue(numJobs);
                            dvds[7].setValue(successfulJobs);
                            dvds[8].setValue(numFailedJobs);
                            dvds[9].setValue(numCancelledJobs);
                            dvds[10].setValue(startTimeMs);
                            dvds[11].setValue(stopTimeMs);
                            dvds[12].setValue(stopTimeMs-startTimeMs);
                            dvds[13].setValue(minJobTime);
                            dvds[14].setValue(maxJobTime);
                            dvds[15].setValue(avgJobTime);

                            rows.add(dataTemplate.getClone());
                        } catch (StandardException e) {
                            throw PublicAPI.wrapStandardException(e);
                        }
                    }
                }


                //TODO -sf- make static
                ResultColumnDescriptor[] columns = new ResultColumnDescriptor[16];
                columns[0] = new GenericColumnDescriptor("statementUuid",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[1] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columns[2] = new GenericColumnDescriptor("userName",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columns[3] = new GenericColumnDescriptor("transactionId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columns[4] = new GenericColumnDescriptor("status",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columns[5] = new GenericColumnDescriptor("sql",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columns[6] = new GenericColumnDescriptor("numJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columns[7] = new GenericColumnDescriptor("successfulJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columns[8] = new GenericColumnDescriptor("numFailedJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columns[9] = new GenericColumnDescriptor("numCancelledJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columns[10] = new GenericColumnDescriptor("startTimeMs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[11] = new GenericColumnDescriptor("stopTimeMs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[12] = new GenericColumnDescriptor("elapsedTimeMs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[13] = new GenericColumnDescriptor("maxJobTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[14] = new GenericColumnDescriptor("minJobTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columns[15] = new GenericColumnDescriptor("avgJobTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE));
                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columns,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

                resultSets[0] = ers;
            }
        });
    }

    public static void SYSCS_GET_STATEMENT_SUMMARY(final ResultSet[] resultSets) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException {
                List<Pair<String, StatementManagement>> statementManagers = JMXUtils.getStatementManagers(jmxConnector);

                ExecRow template = new ValueRow(9);
                template.setRowArray(new DataValueDescriptor[]{
                        new SQLLongint(),new SQLVarchar(),new SQLVarchar(),new SQLLongint(),new SQLVarchar(),new SQLInteger(),new SQLInteger(),new SQLInteger(),new SQLLongint()
                });

                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(statementManagers.size());
                for (Pair<String, StatementManagement> managementPair : statementManagers) {
                    StatementManagement management = managementPair.getSecond();
                    Collection<StatementInfo> completedStatements = management.getExecutingStatementInfo();

                    for (StatementInfo completedStatement : completedStatements) {

                        // Check for null.  My understanding was that ConcurrentHashMap would guarantee that the element would be there.
                        // It appears that the guarantee is that the key element will be there but not necessarily the value element.
                        // This was found to be true when running the ITs in parallel.  DB-1342
                        if (completedStatement == null) { continue; }

                        Set<JobInfo> completedJobs = completedStatement.getCompletedJobs();
                        Set<JobInfo> runningJobs = completedStatement.getRunningJobs();
                        template.resetRowArray();
                        DataValueDescriptor[] dvds = template.getRowArray();
                        try{
                            dvds[0].setValue(completedStatement.getStatementUuid());
                            dvds[1].setValue(managementPair.getFirst());
                            dvds[2].setValue(completedStatement.getUser());
                            dvds[3].setValue(completedStatement.getTxnId());
                            dvds[4].setValue(completedStatement.getSql());
                            dvds[5].setValue(completedStatement.getNumJobs());
                            dvds[6].setValue(completedJobs!=null?completedJobs.size():0);
                            dvds[7].setValue(runningJobs!=null?runningJobs.size():0);
                            dvds[8].setValue(completedStatement.getStartTimeMs());
                        }catch(StandardException se){
                            throw PublicAPI.wrapStandardException(se);
                        }
                        rows.add(template.getClone());
                    }
                }

                ResultColumnDescriptor []columnInfo = new ResultColumnDescriptor[9];
                columnInfo[0] = new GenericColumnDescriptor("statementUuid",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[1] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columnInfo[2] = new GenericColumnDescriptor("userName",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columnInfo[3] = new GenericColumnDescriptor("transactionid",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[4] = new GenericColumnDescriptor("sql",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columnInfo[5] = new GenericColumnDescriptor("numJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[6] = new GenericColumnDescriptor("completedJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[7] = new GenericColumnDescriptor("runningJobs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[8] = new GenericColumnDescriptor("startTimeMs",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));


                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

                resultSets[0] = ers;
            }
        });
    }

    public static void SYSCS_KILL_STATEMENT(final long statementUuid) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<Pair<String, StatementManagement>> statementManagers = JMXUtils.getStatementManagers(connections);
                boolean found = false;
                for (Pair<String, StatementManagement> statementManagementPair : statementManagers) {
                                /*
								 * We don't know which server is actually executing the statement (or even
								 * if it's still running), so just send it out to everyone.
								 */
                    if (statementManagementPair.getSecond().killStatement(statementUuid)) {
                        found = true;	
                    }
                }
                
                // Executing statement not found, so throw exception. Typical user error
				// would be to pass in a transaction id, not a statement uuid.
				if (!found) {
					throw new SQLException(String.format("Executing statement not found with statementUuid = %s", statementUuid));
				}
            }
        });
    }

    public static void SYSCS_GET_WRITE_PIPELINE_INFO(ResultSet[] resultSets) throws SQLException{
        PipelineAdmin.SYSCS_GET_WRITE_PIPELINE_INFO(resultSets);
    }

    public static void SYSCS_GET_WRITE_INTAKE_INFO(ResultSet[] resultSets) throws SQLException{
        PipelineAdmin.SYSCS_GET_WRITE_INTAKE_INFO(resultSets);
    }

    public static void SYSCS_KILL_TRANSACTION(final long transactionId) throws SQLException {
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killTransaction(transactionId);
    }

    public static void SYSCS_KILL_STALE_TRANSACTIONS(final long maximumTransactionId) throws SQLException {
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killAllActiveTransactions(maximumTransactionId);
    }

    public static void SYSCS_GET_MAX_TASKS(final int workerTier, final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<StealableTaskSchedulerManagement> taskSchedulers = JMXUtils.getTieredSchedulerManagement(workerTier, connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (StealableTaskSchedulerManagement taskScheduler : taskSchedulers) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s',%d)",
                            connections.get(i).getFirst(),
                            taskScheduler.getCurrentWorkers()));
                    i++;
                }
                sb.append(") foo (hostname, maxTaskWorkers)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_GLOBAL_MAX_TASKS(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<TieredSchedulerManagement> taskSchedulers = JMXUtils.getTaskSchedulerManagement(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (TieredSchedulerManagement taskScheduler : taskSchedulers) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s',%d)",
                            connections.get(i).getFirst(),
                            taskScheduler.getTotalWorkerCount()));
                    i++;
                }
                sb.append(") foo (hostname, maxTaskWorkers)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_SET_WRITE_POOL(final int writePool) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
                for (ThreadPoolStatus threadPool : threadPools) {
                    threadPool.setMaxThreadCount(writePool);
                }
            }
        });

    }

    public static void SYSCS_GET_WRITE_POOL(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (ThreadPoolStatus threadPool : threadPools) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s',%d)",
                            connections.get(i).getFirst(),
                            threadPool.getMaxThreadCount()));
                    i++;
                }
                sb.append(") foo (hostname, maxTaskWorkers)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_REGION_SERVER_TASK_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<TieredSchedulerManagement> taskSchedulers = JMXUtils.getTaskSchedulerManagement(connections);

                ExecRow template = new ValueRow(13);
                template.setRowArray(new DataValueDescriptor[]{
                        new SQLVarchar(),new SQLInteger(),new SQLInteger(),new SQLInteger(),
                        new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLLongint(),
                        new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLInteger(), new SQLInteger()
                });
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(taskSchedulers.size());

                int i = 0;
                for (TieredSchedulerManagement taskSchedule : taskSchedulers) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try {
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(taskSchedule.getTotalWorkerCount());
                        dvds[2].setValue(taskSchedule.getPending());
                        dvds[3].setValue(taskSchedule.getExecuting());
                        dvds[4].setValue(taskSchedule.getTotalSubmittedTasks());
                        dvds[5].setValue(taskSchedule.getTotalCompletedTasks());
                        dvds[6].setValue(taskSchedule.getTotalFailedTasks());
                        dvds[7].setValue(taskSchedule.getTotalCancelledTasks());
                        dvds[8].setValue(taskSchedule.getTotalInvalidatedTasks());
                        dvds[9].setValue(taskSchedule.getTotalShruggedTasks());
                        dvds[10].setValue(taskSchedule.getTotalStolenTasks());
                        dvds[11].setValue(taskSchedule.getMostLoadedTier());
                        dvds[12].setValue(taskSchedule.getLeastLoadedTier());
                    } catch (StandardException e) {
                        throw PublicAPI.wrapStandardException(e);
                    }

                    rows.add(template.getClone());
                    i++;
                }

                ResultColumnDescriptor []columnInfo = new ResultColumnDescriptor[13];
                columnInfo[0] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columnInfo[1] = new GenericColumnDescriptor("totalWorkers",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[2] = new GenericColumnDescriptor("pending",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[3] = new GenericColumnDescriptor("running",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[4] = new GenericColumnDescriptor("totalSubmitted",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[5] = new GenericColumnDescriptor("totalCompleted",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[6] = new GenericColumnDescriptor("totalFailed",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[7] = new GenericColumnDescriptor("totalCancelled",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[8] = new GenericColumnDescriptor("totalInvalidated",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[9] = new GenericColumnDescriptor("totalShrugged",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[10] = new GenericColumnDescriptor("totalStolen",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[11] = new GenericColumnDescriptor("mostLoadedTier",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[12] = new GenericColumnDescriptor("leastLoadedTier",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

                resultSet[0] = ers;
//                sb.append(") foo (hostname, totalWorkers, pending, running, totalCancelled, "
//                        + "totalCompleted, totalFailed, totalInvalidated, totalSubmitted,totalShrugged,totalStolen,mostLoadedTier,leastLoadedTier)");
//                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_REGION_SERVER_STATS_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {

                ObjectName regionServerStats = null;
                try {
                    regionServerStats = JMXUtils.getRegionServerStatistics();
                } catch (MalformedObjectNameException e) {
                    throw new SQLException(e);
                }
                ExecRow template = new ValueRow(9);
                template.setRowArray(new DataValueDescriptor[]{
                        new SQLVarchar(),new SQLInteger(),new SQLLongint(),new SQLLongint(),
                        new SQLLongint(),new SQLLongint(),new SQLReal(),new SQLInteger(),new SQLInteger()
                });
                int i = 0;
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(connections.size());
                for (Pair<String, JMXConnector> mxc : connections) {
                    MBeanServerConnection mbsc = mxc.getSecond().getMBeanServerConnection();

                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(((Integer)mbsc.getAttribute(regionServerStats,"regions")).intValue());
                        dvds[2].setValue(((Long)mbsc.getAttribute(regionServerStats,"fsReadLatencyAvgTime")).longValue());
                        dvds[3].setValue(((Long)mbsc.getAttribute(regionServerStats,"fsWriteLatencyAvgTime")).longValue());
                        dvds[4].setValue(((Long)mbsc.getAttribute(regionServerStats,"writeRequestsCount")).longValue());
                        dvds[5].setValue(((Long)mbsc.getAttribute(regionServerStats,"readRequestsCount")).longValue());
                        dvds[6].setValue(((Float)mbsc.getAttribute(regionServerStats,"requests")).floatValue());
                        dvds[7].setValue(((Integer)mbsc.getAttribute(regionServerStats,"compactionQueueSize")).intValue());
                        dvds[8].setValue(((Integer)mbsc.getAttribute(regionServerStats,"flushQueueSize")).intValue());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    } catch (Exception e) {
                        throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
                    }
                    rows.add(template.getClone());
                    i++;
                }
                ResultColumnDescriptor []columnInfo = new ResultColumnDescriptor[9];
                columnInfo[0] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
                columnInfo[1] = new GenericColumnDescriptor("regions",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[2] = new GenericColumnDescriptor("fsReadLatencyAvgTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[3] = new GenericColumnDescriptor("fsWriteLatencyAvgTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[4] = new GenericColumnDescriptor("writeRequestsCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[5] = new GenericColumnDescriptor("readRequestsCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
                columnInfo[6] = new GenericColumnDescriptor("requests",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.REAL));
                columnInfo[7] = new GenericColumnDescriptor("compactionQueueSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
                columnInfo[8] = new GenericColumnDescriptor("flushQueueSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
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

    public static void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException {
    	derbyFactory.SYSCS_GET_REQUESTS(resultSet);
            }

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(String schemaName) throws SQLException {
        // sys query for all table conglomerates in schema
        SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(schemaName, null);
    }

    /**
     * Perform a major compaction
     * @param schemaName the name of the database schema to discriminate the tablename.  If null,
     *                   defaults to the 'APP' schema.
     * @param tableName the table name on which to run compaction. If null, compaction will be run
     *                  on all tables in the schema.  Note that a given tablename can produce more
     *                  than one table, if the table has an index, for instance.
     * @throws SQLException
     */
    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(String schemaName, String tableName)
            throws SQLException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // sys query for table conglomerate for in schema
            for (long conglomID : getConglomids(getDefaultConn(), schemaName, tableName)) {
                try {
                    admin.majorCompact(Bytes.toBytes(Long.toString(conglomID)));
                } catch (Exception e) {
                    SpliceLogUtils.warn(LOG, "SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE failed on %s with this message %s, waiting two seconds and will try again", Long.toString(conglomID),e.getMessage());
                    try {
                        Thread.sleep(2000);
                        admin.majorCompact(Bytes.toBytes(Long.toString(conglomID)));
                    } catch (Exception secondE) {
                        SpliceLogUtils.warn(LOG, "SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE failed on %s with this message %s after waiting 2 seconds, compaction attempt aborted", Long.toString(conglomID),e.getMessage());
                    }
                }
            }
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public static void VACUUM() throws SQLException{
        Vacuum vacuum = new Vacuum(getDefaultConn());
        try{
            vacuum.vacuumDatabase();
        }finally{
            vacuum.shutdown();
        }
    }
    public static ResultColumnDescriptor[] SCHEMA_INFO_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("SCHEMANAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("TABLENAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("REGIONNAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("IS_INDEX",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
            new GenericColumnDescriptor("HBASEREGIONS_STORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("MEMSTORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("STOREINDEXSIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };
    public static void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException {
    	derbyFactory.SYSCS_GET_SCHEMA_INFO(resultSet);
            }

    /**
     * Prints all the information related to the execution plans of the stored prepared statements (metadata queries).
     */
    public static void SYSCS_GET_STORED_STATEMENT_PLAN_INFO(ResultSet[] rs) throws SQLException
    {
        try {
            // Wow...  who knew it was so much work to create a ResultSet?  Ouch!  The following code is annoying.

            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            DataDictionary dd = lcc.getDataDictionary();
            List list = dd.getAllSPSDescriptors();
            ArrayList<ExecRow> rows = new ArrayList<ExecRow>(list.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   STMTNAME				VARCHAR
            //   TYPE					CHAR
            //   VALID					BOOLEAN
            //   LASTCOMPILED			TIMESTAMP
            //   INITIALLY_COMPILABLE	BOOLEAN
            //   CONSTANTSTATE			BLOB --> VARCHAR showing existence of plan
            DataValueDescriptor[] dvds = new DataValueDescriptor[] {
                    new SQLVarchar(),
                    new SQLChar(),
                    new SQLBoolean(),
                    new SQLTimestamp(),
                    new SQLBoolean(),
                    new SQLVarchar()
            };
            int numCols = dvds.length;
            ExecRow dataTemplate = new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the descriptors into the rows.
            for (Iterator li = list.iterator(); li.hasNext(); )
            {
                SPSDescriptor spsd = (SPSDescriptor) li.next();
                ExecPreparedStatement ps = spsd.getPreparedStatement(false);
                dvds[0].setValue(spsd.getName());
                dvds[1].setValue(spsd.getTypeAsString());
                dvds[2].setValue(spsd.isValid());
                dvds[3].setValue(spsd.getCompileTime());
                dvds[4].setValue(spsd.initiallyCompilable());
                dvds[5].setValue(spsd.getPreparedStatement(false) == null ? null : "[object]");
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[numCols];
            columnInfo[0] = new GenericColumnDescriptor("STMTNAME", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 60));
            columnInfo[1] = new GenericColumnDescriptor("TYPE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 4));
            columnInfo[2] = new GenericColumnDescriptor("VALID", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[3] = new GenericColumnDescriptor("LASTCOMPILED", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP));
            columnInfo[4] = new GenericColumnDescriptor("INITIALLY_COMPILABLE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[5] = new GenericColumnDescriptor("CONSTANTSTATE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 13));
            EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo, lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);
            rs[0] = ers;
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Get the values of all properties for the current connection.
     *
     *  @param rs array of result set objects that contains all of the defined properties
     *            for the JVM, service, database, and app.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_GET_ALL_PROPERTIES(ResultSet[] rs) throws SQLException
    {
        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            TransactionController tc = lcc.getTransactionExecute();

            // Fetch all the properties.
            Properties jvmProps = addTypeToProperties(System.getProperties(), "JVM", true);
            Properties dbProps = addTypeToProperties(tc.getProperties());  // Includes both database and service properties.
            ModuleFactory monitor = Monitor.getMonitorLite();
            Properties appProps = addTypeToProperties(monitor.getApplicationProperties(), "APP", false);

            // Merge the properties using the correct search order.
            // SEARCH ORDER: JVM, Service, Database, App
            appProps.putAll(dbProps);  // dbProps already has been overwritten with service properties.
            appProps.putAll(jvmProps);
            ArrayList<ExecRow> rows = new ArrayList<ExecRow>(appProps.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   KEY			VARCHAR
            //   VALUE			VARCHAR
            //   TYPE			VARCHAR (JVM, SERVICE, DATABASE, APP)
            DataValueDescriptor[] dvds = new DataValueDescriptor[] {
                    new SQLVarchar(),
                    new SQLVarchar(),
                    new SQLVarchar()
            };
            int numCols = dvds.length;
            ExecRow dataTemplate = new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the properties into rows.  Sort the properties by key first.
            ArrayList<String> keyList = new ArrayList(appProps.keySet());
            Collections.sort(keyList);
            for (Iterator<String> iter = keyList.iterator(); iter.hasNext(); )
            {
                String key = (String) iter.next();
                String[] typedValue = (String[]) appProps.get(key);
                dvds[0].setValue(key);
                dvds[1].setValue(typedValue[0]);
                dvds[2].setValue(typedValue[1]);
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[numCols];
            columnInfo[0] = new GenericColumnDescriptor("KEY", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 50));
            columnInfo[1] = new GenericColumnDescriptor("VALUE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 40));
            columnInfo[2] = new GenericColumnDescriptor("TYPE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 10));
            EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo, lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);
            rs[0] = ers;
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Tag each property value with a 'type'.  The structure of the map changes from:
     *     {key --> value}
     * to:
     *     {key --> [value, type]}
     *
     * @param props				Map of properties to tag with type
     * @param type				Type of property (JVM, SERVICE, DATABASE, APP, etc.)
     * @param derbySpliceOnly	If true, only include properties with keys that start with "derby" or "splice".
     *
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props, String type, boolean derbySpliceOnly) {
        Properties typedProps = new Properties();
        if (props != null) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); )
            {
                Map.Entry prop = (Map.Entry) iter.next();
                String key = (String) prop.getKey();
                if (derbySpliceOnly && key != null) {
                    String lowerKey = key.toLowerCase();
                    if (!lowerKey.startsWith("derby") && !lowerKey.startsWith("splice")) {
                        continue;
                    }
                }
                String[] typedValue = new String[2];
                typedValue[0] = (String) prop.getValue();
                typedValue[1] = type;
                typedProps.put(key, typedValue);
            }
        }
        return typedProps;
    }

    /**
     * Tag each property value with a 'type' of SERVICE or DATABASE.  The structure of the map changes from:
     *     {key --> value}
     * to:
     *     {key --> [value, type]}
     *
     * @param props  Map of properties to tag with type
     *
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props) {
        Properties typedProps = new Properties();
        if (props != null) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); )
            {
                Map.Entry prop = (Map.Entry) iter.next();
                String key = (String) prop.getKey();
                String[] typedValue = new String[2];
                typedValue[0] = (String) prop.getValue();
                typedValue[1] = PropertyUtil.isServiceProperty(key) ? "SERVICE" : "DATABASE";
                typedProps.put(key, typedValue);
            }
        }
        return typedProps;
    }

    /**
     * Be Careful when using this, as it will return conglomerate ids for all the indices of a table
     * as well as the table itself. While the first conglomerate SHOULD be the main table, there
     * really isn't a guarantee, and it shouldn't be relied upon for correctness in all cases.
     */
    public static long[] getConglomids(Connection conn, String schemaName, String tableName) throws SQLException {
        List<Long> conglomIDs = new ArrayList<Long>();
        if (schemaName == null)
            // default schema
            schemaName = "APP";

        String allTablesInSchema =  "SELECT C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
                "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID AND S.SCHEMANAME = ?";

        String query =  "SELECT C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
                "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID AND S.SCHEMANAME = ? AND T.TABLENAME = ?";

        if (tableName == null)
            // all tables in schema
            query = allTablesInSchema;

        ResultSet rs = null;
        PreparedStatement s = null;
        try {
            s = conn.prepareStatement(query);
            s.setString(1, schemaName.toUpperCase());
            if (tableName != null) {
                s.setString(2, tableName.toUpperCase());
            }
            rs = s.executeQuery();
            while (rs.next()) {
                conglomIDs.add(rs.getLong(1));
            }
            if (conglomIDs.isEmpty()) {
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            }
        } finally {
            if (rs != null) rs.close();
            if (s != null) s.close();
        }
        if (conglomIDs.isEmpty()) {
            return new long[0];
        }
        long[] congloms = new long[conglomIDs.size()];
        for (int i =0; i<conglomIDs.size(); i++) {
            congloms[i] = conglomIDs.get(i);
        }
				/*
				 * An index conglomerate id can be returned by the query before the main table one is,
				 * but it should ALWAYS have a higher conglomerate id, so if we sort the congloms,
				 * we should return the main table before any of its indices.
				 */
        Arrays.sort(congloms);
        return congloms;
    }

    private static class Trip<T, U, V> {
        private final T first;
        private final U second;
        private final V third;

        public Trip(T first, U second, V third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }

        public V getThird() {
            return third;
        }
    }

    /*
     * Implementation for SYSCS_UTIL.XPLAIN_TRACE system procedure
     *
     * @param    statementId : unique identifier for the sql statement
     * @param    mode        : 0 - operation tree only
     *                         1 - execution plan with metrics
     * @param    format      : 'TREE' - Text-based tree representation
     *                         'JSON' - JSON document of nested operations
     * @return   an execution plan in a result set
     *
     */
    public static void SYSCS_GET_XPLAIN_TRACE(long statementId, int mode, String format, final ResultSet[] resultSet) throws Exception{
        XPlainTrace xPlainTrace = new XPlainTrace(statementId, mode, format);
        resultSet[0] = xPlainTrace.getXPlainTraceOutput();
    }

    public static void SYSCS_GET_XPLAIN_STATEMENTID(final ResultSet[] resultSet) throws Exception{
        EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = defaultConn.getLanguageConnection();
        long statementId = lcc.getXplainStatementId();
        List<ExecRow> rows = Lists.newArrayListWithExpectedSize(1);

        ExecRow row = new ValueRow(1);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLLongint()});
        DataValueDescriptor[] dvds = row.getRowArray();
        dvds[0].setValue(statementId);
        rows.add(row);

        ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[1];
        columnInfo[0] = new GenericColumnDescriptor("STATEMENTID", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));

        Activation lastActivation = lcc.getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

        resultSet[0] = ers;
    }

    public static void SYSCS_GET_RUNTIME_STATISTICS(final ResultSet[] resultSet) throws Exception{
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        boolean runTimeStatisticsMode = lcc.getRunTimeStatisticsMode();

        List<ExecRow> rows = Lists.newArrayListWithExpectedSize(1);

        ExecRow row = new ValueRow(1);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLBoolean()});
        DataValueDescriptor[] dvds = row.getRowArray();
        dvds[0].setValue(runTimeStatisticsMode);
        rows.add(row);

        ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[1];
        columnInfo[0] = new GenericColumnDescriptor("RUNTIMESTATISTICS", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));

        EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

        resultSet[0] = ers;
    }

    public static void SYSCS_GET_STATISTICS_TIMING(final ResultSet[] resultSet) throws Exception{
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        boolean statisticsTiming = lcc.getStatisticsTiming();

        List<ExecRow> rows = Lists.newArrayListWithExpectedSize(1);

        ExecRow row = new ValueRow(1);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLBoolean()});
        DataValueDescriptor[] dvds = row.getRowArray();
        dvds[0].setValue(statisticsTiming);
        rows.add(row);

        ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[1];
        columnInfo[0] = new GenericColumnDescriptor("STATISTICSTIMING", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));

        EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

        resultSet[0] = ers;
    }

    public static void SYSCS_PURGE_XPLAIN_TRACE() throws SQLException{
        Connection connection = SpliceAdmin.getDefaultConn();
        PreparedStatement s = connection.prepareStatement("delete from sys.sysstatementhistory");
        s.execute();

        s = connection.prepareStatement("delete from sys.sysoperationhistory");
        s.execute();

        s = connection.prepareStatement("delete from sys.systaskhistory");
        s.execute();
    }

    public static void SYSCS_SET_AUTO_TRACE(int enable) throws SQLException, StandardException {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        lcc.setAutoTrace(enable==0?false:true);
    }

    public static boolean SYSCS_GET_AUTO_TRACE() throws Exception{
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        boolean isAutoTraced = lcc.isAutoTraced();

        return isAutoTraced;
    }

    public static void SYSCS_SET_XPLAIN_TRACE(int enable) throws SQLException, StandardException {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        lcc.setRunTimeStatisticsMode(enable != 0 ? true : false);
        lcc.setStatisticsTiming(enable != 0 ? true : false);
    }
}
