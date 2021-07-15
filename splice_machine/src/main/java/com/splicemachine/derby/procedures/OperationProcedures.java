package com.splicemachine.derby.procedures;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.impl.drda.RemoteUser;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.iapi.sql.execute.OperationManager;
import com.splicemachine.derby.iapi.sql.execute.RunningOperation;
import com.splicemachine.derby.utils.ResultHelper;
import com.splicemachine.utils.Pair;
import splice.com.google.common.net.HostAndPort;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INVALID_FUNCTION_ARGUMENT;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_NO_SUCH_RUNNING_OPERATION;

public class OperationProcedures extends BaseAdminProcedures {
    public static void addProcedures(List<Procedure> procedures) {
        /*
         * Procedure to get a list of running operations
         */
        Procedure runningOperations = Procedure.newBuilder().name("SYSCS_GET_RUNNING_OPERATIONS")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(runningOperations);

        /*
         * Procedure to get a list of running operations on the local server
         */
        Procedure runningOperationsLocal = Procedure.newBuilder().name("SYSCS_GET_RUNNING_OPERATIONS_LOCAL")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(runningOperationsLocal);

        /*
         * Procedure to get a list of progress of running operations
         */
        Procedure getProgress = Procedure.newBuilder().name("SYSCS_GET_PROGRESS")
                .numOutputParams(0)
                .varchar("drdaTokenFilter",64)
                .integer("useDrdaToken")
                .numResultSets(1)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(getProgress);

        /*
         * Procedure to get a list of progress of running operations on the local server
         */
        Procedure getProgressLocal = Procedure.newBuilder().name("SYSCS_GET_PROGRESS_LOCAL")
                .numOutputParams(0)
                .varchar("drdaTokenFilter",64)
                .integer("useDrdaToken")
                .numResultSets(1)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(getProgressLocal);

        /*
         * Procedure to kill an executing operation
         */
        Procedure killOperationLocal = Procedure.newBuilder().name("SYSCS_KILL_OPERATION_LOCAL")
                .numOutputParams(0)
                .varchar("uuid",128)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(killOperationLocal);

        /*
         * Procedure to kill an executing operation
         */
        Procedure killOperation = Procedure.newBuilder().name("SYSCS_KILL_OPERATION")
                .numOutputParams(0)
                .varchar("uuid",128)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(killOperation);

        /*
         * Procedure to kill an executing DRDA operation
         */
        Procedure killDrdaOperationLocal = Procedure.newBuilder().name("SYSCS_KILL_DRDA_OPERATION_LOCAL")
                .numOutputParams(0)
                .varchar("rdbIntTkn",512)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(killDrdaOperationLocal);

        /*
         * Procedure to kill an executing DRDA operation
         */
        Procedure killDrdaOperation = Procedure.newBuilder().name("SYSCS_KILL_DRDA_OPERATION")
                .numOutputParams(0)
                .varchar("rdbIntTkn",512)
                .ownerClass(OperationProcedures.class.getCanonicalName())
                .build();
        procedures.add(killDrdaOperation);
    }

    public static void SYSCS_GET_RUNNING_OPERATIONS(final ResultSet[] resultSet) throws SQLException {
        String sql = "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS_LOCAL()";
        resultSet[0] = getRunningOperations(sql, false, false ).getResultSet();
    }

    public static void SYSCS_GET_PROGRESS(String drdaToken, Integer getDRDA, final ResultSet[] resultSet) throws SQLException{
        String sql = "call SYSCS_UTIL.SYSCS_GET_PROGRESS_LOCAL('" + drdaToken + "', '" + getDRDA + "')";
        resultSet[0] = getRunningOperations(sql, getDRDA != 0, true).getResultSet();
    }
    private static ResultHelperRunningOps getRunningOperations(String sql, boolean drda, boolean progress) throws SQLException {
        ResultHelperRunningOps res = new ResultHelperRunningOps(drda, progress);
        executeOnAllServers(sql, (hostAndPort, connection, rs) -> {
            while (rs.next()) {
                if (sql.equalsIgnoreCase(rs.getString(5))) {
                    // Filter out the nested calls to SYSCS_GET_RUNNING_OPERATIONS_LOCAL
                    // triggered by this stored procedure
                    continue;
                }
                res.newRowFromResultSet(rs);
            }
        } );
        return res;
    }

    public static void SYSCS_GET_RUNNING_OPERATIONS_LOCAL(final ResultSet[] resultSet) throws SQLException{
        getRunningOperationsOrProgressLocal(null, resultSet, false, false);
    }

    public static void SYSCS_GET_PROGRESS_LOCAL(String drdaToken, Integer getDRDA, final ResultSet[] resultSet) throws SQLException{
        getRunningOperationsOrProgressLocal(drdaToken, resultSet, getDRDA != 0, true);
    }

    static class ResultHelperRunningOps extends ResultHelper {
        VarcharColumn colUuid;
        VarcharColumn colUser;
        VarcharColumn colHostname;
        IntegerColumn colSession;
        VarcharColumn colSql;
        VarcharColumn colSubmitted;
        VarcharColumn colElapsed;
        VarcharColumn colEngine;
        VarcharColumn ownerStateCol;

        ResultHelperRunningOps(boolean drda, boolean progress) {
            colUuid       = addVarchar(drda ? "DRDA_TOKEN" : "UUID", 40);
            colUser       = addVarchar("USER", 40);
            colHostname   = addVarchar("HOSTNAME", 40);
            colSession    = addInteger("SESSION");
            colSql        = addVarchar("SQL", 40);
            colSubmitted  = addVarchar("SUBMITTED", 40);
            colElapsed    = addVarchar("ELAPSED", 40);
            colEngine     = addVarchar("ENGINE", 40);
            ownerStateCol = progress ? addVarchar("STATE", 100) : addVarchar("OWNER", 100);
        }
    }

    public static void getRunningOperationsOrProgressLocal(String drdaToken, final ResultSet[] resultSet,
                                                           boolean drda, boolean progress) throws SQLException{
        EmbedConnection conn = (EmbedConnection)getDefaultConn();
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        String userId = lastActivation.getLanguageConnectionContext().getCurrentUserId(lastActivation);
        String dbo = lastActivation.getLanguageConnectionContext().getCurrentDatabase().getAuthorizationId();
        if (userId != null && userId.equals(dbo)) {
            userId = null;
        }

        OperationManager om = EngineDriver.driver().getOperationManager();
        // GET_PROGRESS needs DRDA UUID (rdbIntTkn / drda connection token), while GET_RUNNING_OPERATIONS needs
        // our custom UUID.
        List<Pair<String, RunningOperation>> operations;
        if(drda)
            operations = om.runningOperationsDRDA(userId, drdaToken);
        else
            operations = om.runningOperations(userId);

        SConfiguration config=EngineDriver.driver().getConfiguration();
        String host_port = NetworkUtils.getHostname(config) + ":" + config.getNetworkBindPort();
        final String timeStampFormat = SQLTimestamp.defaultTimestampFormatString;

        ResultHelperRunningOps res = new ResultHelperRunningOps(drda, progress);

        for (Pair<String, RunningOperation> pair : operations)
        {
            String uuid = pair.getFirst();
            RunningOperation ro = pair.getSecond();
            Activation activation = ro.getOperation().getActivation();
            assert activation.getPreparedStatement() != null : "Prepared Statement is null";
            ExecPreparedStatement ps = activation.getPreparedStatement();

            res.newRow();
            res.colUuid     .set(uuid);
            res.colUser     .set(activation.getLanguageConnectionContext().getCurrentUserId(activation));
            res.colHostname .set(host_port);
            res.colSession  .set(activation.getLanguageConnectionContext().getInstanceNumber());
            res.colSql      .set(ps == null ? null : ps.getSource());
            res.colSubmitted.set(new SimpleDateFormat(timeStampFormat).format(ro.getSubmittedTime()));
            res.colElapsed  .set(getElapsedTimeStr(ro.getSubmittedTime(), new Date()));
            res.colEngine   .set(ro.getEngineName());
            if(progress)
                res.ownerStateCol.set(ro.getProgressString() == null ? "-" : ro.getProgressString()); // State
            else
                res.ownerStateCol.set(ro.getOperation().getScopeName()); // JOBTYPE
        }
        resultSet[0] = res.getResultSet();
    }


    private static String getElapsedTimeStr(Date begin, Date end)
    {
        long between  = (end.getTime() - begin.getTime()) / 1000;
        long day = between / (24 * 3600);
        long hour = between % (24 * 3600) / 3600;
        long minute = between % 3600 / 60;
        long second = between % 60;
        StringBuilder elapsedStr = new StringBuilder();
        if (day > 0) {
            elapsedStr.append(day + " day(s) ").append(hour + " hour(s) ").append(minute + " min(s) ").append(second + " sec(s)");
        } else if (hour > 0) {
            elapsedStr.append(hour + " hour(s) ").append(minute + " min(s) ").append(second + " sec(s)");
        } else if (minute > 0) {
            elapsedStr.append(minute + " min(s) ").append(second + " sec(s)");
        } else {
            elapsedStr.append(second + " sec(s)");
        }
        return elapsedStr.toString();
    }

    public static void SYSCS_KILL_DRDA_OPERATION(final String token) throws SQLException {
        String[] parts = token.split("#");
        String uuidString = parts[0];
        String hostname = parts[1];
        HostAndPort needle = null;
        for (HostAndPort hap : getServers() ) {
            if (hap.toString().equals(hostname)) {
                needle = hap;
                break;
            }
        }
        if (needle == null)
            throw  PublicAPI.wrapStandardException(StandardException.newException(LANG_NO_SUCH_RUNNING_OPERATION, token));

        try (Connection connection = RemoteUser.getConnection(hostname)) {
            try (PreparedStatement ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_KILL_DRDA_OPERATION_LOCAL(?)")) {
                ps.setString(1, uuidString);
                ps.execute();
            }
        }
    }

    public static void SYSCS_KILL_OPERATION(final String uuidString) throws SQLException {
        ExecRow needle = null;
        String sql = "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS_LOCAL()";
        for (ExecRow row : getRunningOperations(sql, false,false).getExecRows()) {
            try {
                if (row.getColumn(1).getString().equals(uuidString)) {
                    needle = row;
                    break;
                }
            } catch (StandardException se) {
                throw PublicAPI.wrapStandardException(se);
            }
        }
        if (needle == null)
            throw  PublicAPI.wrapStandardException(StandardException.newException(LANG_NO_SUCH_RUNNING_OPERATION, uuidString));

        try {
            String server = needle.getColumn(3).getString();

            try (Connection connection = RemoteUser.getConnection(server)) {
                try (PreparedStatement ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_KILL_OPERATION_LOCAL(?)")) {
                    ps.setString(1, uuidString);
                    ps.execute();
                }
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    static void killLocal(String id, boolean drda) throws SQLException {
        EmbedConnection conn = (EmbedConnection)getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();

        String userId = lcc.getCurrentUserId(lastActivation);

        boolean killed;
        try {
            OperationManager om = EngineDriver.driver().getOperationManager();
            if(drda) {
                killed = om.killDRDAOperation(id, userId);
            }
            else {
                UUID uuid;
                try {
                    uuid = UUID.fromString(id);
                } catch (IllegalArgumentException e) {
                    throw PublicAPI.wrapStandardException(StandardException.newException(LANG_INVALID_FUNCTION_ARGUMENT,
                            id, "SYSCS_KILL_OPERATION"));
                }
                killed = om.killOperation(uuid, userId);
            }
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }

        if (!killed)
            throw PublicAPI.wrapStandardException(StandardException.newException(LANG_NO_SUCH_RUNNING_OPERATION, id));
    }

    public static void SYSCS_KILL_DRDA_OPERATION_LOCAL(final String rdbIntTkn) throws SQLException {
        killLocal(rdbIntTkn, true);
    }

    public static void SYSCS_KILL_OPERATION_LOCAL(final String uuidString) throws SQLException{
        killLocal(uuidString, false);
    }
}
