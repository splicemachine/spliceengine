package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskSchedulerManagement;
import com.splicemachine.derby.impl.job.scheduler.TieredSchedulerManagement;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.logging.Logging;
import java.io.IOException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.ManifestReader.SpliceMachineVersion;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 12/9/13
 */
public class SpliceAdmin extends BaseAdminProcedures {
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

    public static void SYSCS_GET_WRITE_INTAKE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ActiveWriteHandlersIface> activeWriteHandler = JMXUtils.getActiveWriteHandlers(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (ActiveWriteHandlersIface activeWrite : activeWriteHandler) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s',%d,%d,%d,%d)", connections.get(i).getFirst(),
                                            activeWrite.getActiveWriteThreads(),
                                            activeWrite.getCompactionQueueSizeLimit(),
                                            activeWrite.getFlushQueueSizeLimit(),
                                            activeWrite.getIpcReservedPool()));
                    i++;
                }
                sb.append(") foo (hostname, activeWriteThreads, compactionQueueSizeLimit, flushQueueSizeLimit, ipcReserverdPool)");
                resultSet[0] = executeStatement(sb);
            }
        });
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

    public static void SYSCS_GET_ACTIVE_JOB_IDS(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                // <jobID-><statement,jobHost>>
                Map<String, List<Pair<String, String>>> jobMap = new HashMap<String, List<Pair<String, String>>>();
                // <jobID-><taskID,taskHost,status>>
                Map<String, List<Trip<String, String, String>>> taskMap = new HashMap<String, List<Trip<String, String, String>>>();
                List<Pair<String, JobSchedulerManagement>> jobMonitors = JMXUtils.getJobSchedulerManagement(connections);

                for (Pair<String, JobSchedulerManagement> taskMonitor : jobMonitors) {
                    for (String job : taskMonitor.getSecond().getRunningJobs()) {
                        // [jobID,statement]
                        String[] jobComponents = job.split("\\" + JobSchedulerManagement.SEP_CHAR);
                        String jobID = jobComponents[0];
                        List<Pair<String, String>> jobVals = jobMap.get(jobID);
                        if (jobVals == null) {
                            jobVals = new ArrayList<Pair<String, String>>();
                        }
                        jobVals.add(new Pair<String, String>(jobComponents[1], taskMonitor.getFirst()));
                        jobMap.put(jobID, jobVals);
                    }
                    for (String task : taskMonitor.getSecond().getRunningTasks()) {
                        // [jobID,taskID,taskStatus]
                        String[] taskComponents = task.split("\\" + JobSchedulerManagement.SEP_CHAR);
                        String jobID = taskComponents[0];
                        List<Trip<String, String, String>> taskVals = taskMap.get(jobID);
                        if (taskVals == null) {
                            taskVals = new ArrayList<Trip<String, String, String>>();
                        }
                        taskVals.add(new Trip<String, String, String>(taskComponents[1], taskMonitor.getFirst(), taskComponents[2]));
                        taskMap.put(jobID, taskVals);
                    }
                }

                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (Map.Entry<String, List<Pair<String, String>>> jobEntry : jobMap.entrySet()) {
                    String jobID = jobEntry.getKey();
                    for (Pair<String, String> statement : jobEntry.getValue()) {
                        String sql = SpliceUtils.escape(statement.getFirst());
                        String jobHost = statement.getSecond();
                        String taskID = "unknownID";
                        String taskHost = "unknownHost";
                        String taskStatus = "unknownStatus";
                        List<Trip<String, String, String>> taskEntries = taskMap.get(jobID);
                        if (i != 0) {
                            sb.append(", ");
                        }
                        if (taskEntries != null && !taskEntries.isEmpty()) {
                            // todo - does nothing if no task info
                            for (Trip<String, String, String> taskEntry : taskEntries) {
                                taskID = taskEntry.getFirst();
                                taskHost = taskEntry.getSecond();
                                taskStatus = taskEntry.getThird();
                                sb.append(String.format("('%s','%s','%s','%s','%s','%s')",
                                        sql,
                                        jobID,
                                        jobHost,
                                        taskID,
                                        taskHost,
                                        taskStatus));
                            }
                        } else {
                            sb.append(String.format("('%s','%s','%s','%s','%s','%s')",
                                    sql,
                                    jobID,
                                    jobHost,
                                    taskID,
                                    taskHost,
                                    taskStatus));
                        }
                        i++;
                    }
                }
                sb.append(") foo (statement, jobid, jobhost, taskid, taskhost, status)");
                resultSet[0] = executeStatement(sb);
            }
        });
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
                for (Pair<String, StatementManagement> statementManagementPair : statementManagers) {
                                /*
								 * We don't know which server is actually executing the statement (or even
								 * if it's still running), so just send it out to everyone.
								 */
                    statementManagementPair.getSecond().killStatement(statementUuid);
                }
            }
        });
    }

    public static void SYSCS_KILL_TRANSACTION(final long transactionId) throws SQLException {
        try {
            HTransactorFactory.getTransactionManager().fail(new TransactionId(Long.toString(transactionId)));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void SYSCS_KILL_STALE_TRANSACTIONS(final long maximumTransactionId) throws SQLException {
        try {
            TransactionManager transactor = HTransactorFactory.getTransactionManager();
            List<TransactionId> active = transactor.getActiveTransactionIds(new TransactionId(Long.toString(maximumTransactionId)));
            for (TransactionId txnId : active) {
                transactor.fail(txnId);
            }
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
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


    public static void SYSCS_GET_WRITE_PIPELINE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
								ExecRow template = new ValueRow(8);
								template.setRowArray(new DataValueDescriptor[]{
												new SQLVarchar(),new SQLInteger(),new SQLInteger(),new SQLInteger(),new SQLLongint(),new SQLLongint(),new SQLLongint(),new SQLLongint()
								});
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
								ResultColumnDescriptor []columnInfo = new ResultColumnDescriptor[8];
								columnInfo[0] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
								columnInfo[1] = new GenericColumnDescriptor("maxThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
								columnInfo[2] = new GenericColumnDescriptor("activeThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
								columnInfo[3] = new GenericColumnDescriptor("pendingWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
								columnInfo[4] = new GenericColumnDescriptor("totalSubmittedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
								columnInfo[5] = new GenericColumnDescriptor("totalSuccessfulWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
								columnInfo[6] = new GenericColumnDescriptor("totalFailedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
								columnInfo[7] = new GenericColumnDescriptor("totalRejectedWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));

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
        StringBuilder sb = new StringBuilder("select * from (values ");
        int i = 0;
        for (Map.Entry<ServerName, HServerLoad> serverLoad : getLoad().entrySet()) {
            if (i != 0) {
                sb.append(", ");
            }
            ServerName sn = serverLoad.getKey();
            sb.append(String.format("('%s',%d,%d)",
                    sn.getHostname(),
                    sn.getPort(),
                    serverLoad.getValue().getTotalNumberOfRequests()));
            i++;
        }
        sb.append(") foo (hostname, port, totalRequests)");
        resultSet[0] = executeStatement(sb);
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

    public static void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException {
        ResultSet allTablesInSchema = getDefaultConn().prepareStatement("SELECT S.SCHEMANAME, T.TABLENAME, C.ISINDEX, " +
                "C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
                "WHERE C.TABLEID = T.TABLEID AND T.SCHEMAID = S.SCHEMAID AND T.TABLENAME NOT LIKE 'SYS%' " +
                "ORDER BY S.SCHEMANAME").executeQuery();
        StringBuilder sb = new StringBuilder("select * from (values ");

        int nCols = allTablesInSchema.getMetaData().getColumnCount();
        // Map<regionNameAsString,HServerLoad.RegionLoad>
        Map<String, HServerLoad.RegionLoad> regionLoadMap = getRegionLoad();
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            StringBuilder regionBuilder = new StringBuilder();
            while (allTablesInSchema.next()) {
                String conglom = allTablesInSchema.getObject("CONGLOMERATENUMBER").toString();
                regionBuilder.setLength(0);
                for (HRegionInfo ri : admin.getTableRegions(Bytes.toBytes(conglom))) {
                    String regionName = Bytes.toString(ri.getRegionName());
                    if (regionName != null && ! regionName.isEmpty()) {
                        HServerLoad.RegionLoad regionLoad = regionLoadMap.get(regionName);
                        if (regionLoad != null) {
                            int storefileSizeMB = regionLoad.getStorefileSizeMB();
                            int memStoreSizeMB = regionLoad.getMemStoreSizeMB();
                            int storefileIndexSizeMB = regionLoad.getStorefileIndexSizeMB();

                            byte[][] parsedRegionName = HRegionInfo.parseRegionName(ri.getRegionName());
                            String tableName = "Unknown";
                            String regionID = "Unknown";
                            if (parsedRegionName != null) {
                                if (parsedRegionName.length >= 1) {
                                    tableName = Bytes.toString(parsedRegionName[0]);
                                }
                                if (parsedRegionName.length >= 3) {
                                    regionID = Bytes.toString(parsedRegionName[2]);
                                }
                            }
                            regionBuilder.append('(')
                                    .append(tableName).append(',')
                                    .append(regionID).append(' ')
                                    .append(storefileSizeMB).append(' ')
                                    .append(memStoreSizeMB).append(' ')
                                    .append(storefileIndexSizeMB)
                                    .append(" MB) ");
                        }
                    }
                }

                sb.append(String.format("('%s','%s','%s','%s'), ",
                        allTablesInSchema.getObject("SCHEMANAME"),
                        allTablesInSchema.getObject("TABLENAME"),
                        allTablesInSchema.getObject("ISINDEX"),
                        regionBuilder.toString()));
            }
            if (sb.length() > 2) {
                // trim last ', '
                sb.setLength(sb.length()-2);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (allTablesInSchema != null) {
                try {
                    allTablesInSchema.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
        sb.append(") foo (SCHEMANAME, TABLENAME, ISINDEX, HBASEREGIONS_STORESIZE_MEMSTORESIZE_STOREINDEXSIZE)");
        resultSet[0] = executeStatement(sb);
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

    		// Transform the properties into rows.
    		for (Iterator iter = appProps.entrySet().iterator(); iter.hasNext(); )
    		{
    			Map.Entry prop = (Map.Entry) iter.next();
    			String key = (String) prop.getKey();
    			String[] typedValue = (String[]) prop.getValue();
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

    /* @return  Map<regionNameAsString,HServerLoad.RegionLoad> */
    private static Map<String, HServerLoad.RegionLoad> getRegionLoad() throws SQLException {
        Map<String, HServerLoad.RegionLoad> regionLoads = new HashMap<String, HServerLoad.RegionLoad>();
        HBaseAdmin admin = null;
        admin = SpliceUtils.getAdmin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for (ServerName serverName : clusterStatus.getServers()) {
                final HServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], HServerLoad.RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    regionLoads.put(Bytes.toString(entry.getKey()), entry.getValue());
                }
            }
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (admin != null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
        }

        return regionLoads;
    }

    private static Map<ServerName, HServerLoad> getLoad() throws SQLException {
        Map<ServerName, HServerLoad> serverLoadMap = new HashMap<ServerName, HServerLoad>();
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            for (ServerName serverName : SpliceUtils.getServers()) {
                try {
                    serverLoadMap.put(serverName, admin.getClusterStatus().getLoad(serverName));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        } finally {
            if (admin != null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
        }

        return serverLoadMap;
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
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
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
        Connection connection = SpliceDriver.driver().getInternalConnection();
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
}
