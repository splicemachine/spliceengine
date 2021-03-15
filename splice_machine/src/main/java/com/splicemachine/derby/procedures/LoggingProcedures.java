package com.splicemachine.derby.procedures;

import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.utils.ResultHelper;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.logging.Logging;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class LoggingProcedures extends BaseAdminProcedures {
    public static void addProcedures(List<Procedure> procedures) {
        /*
         * Procedure to set the log level for the given logger
         */
        Procedure setLoggerLevel = Procedure.newBuilder().name("SYSCS_SET_LOGGER_LEVEL")
                .numOutputParams(0)
                .numResultSets(0)
                .varchar("loggerName", 128)
                .varchar("loggerLevel", 128)
                .sqlControl(RoutineAliasInfo.NO_SQL)
                .ownerClass(SpliceAdmin.class.getCanonicalName())
                .build();
        procedures.add(setLoggerLevel);

        Procedure setLoggerLevelLocal = Procedure.newBuilder().name("SYSCS_SET_LOGGER_LEVEL_LOCAL")
                .numOutputParams(0)
                .numResultSets(0)
                .varchar("loggerName", 128)
                .varchar("loggerLevel", 128)
                .sqlControl(RoutineAliasInfo.NO_SQL)
                .ownerClass(SpliceAdmin.class.getCanonicalName())
                .build();
        procedures.add(setLoggerLevelLocal);

        /*
         * Procedure to get all the splice logger names in the system
         */
        Procedure getLoggers = Procedure.newBuilder().name("SYSCS_GET_LOGGERS")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(LoggingProcedures.class.getCanonicalName())
                .build().debugCheck();
        procedures.add(getLoggers);

        Procedure getLoggersLocal = Procedure.newBuilder().name("SYSCS_GET_LOGGERS_LOCAL")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(LoggingProcedures.class.getCanonicalName())
                .build().debugCheck();
        procedures.add(getLoggersLocal);
    }

    @SuppressFBWarnings("IIL_PREPARE_STATEMENT_IN_LOOP") // intentional (different servers)
    public static void SYSCS_SET_LOGGER_LEVEL(final String loggerName, final String logLevel) throws SQLException {
        executeOnAllServers( (hostAndPort, connection) -> {
            try (PreparedStatement ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL_LOCAL(?, ?)")) {
                ps.setString(1, loggerName);
                ps.setString(2, logLevel);
                ps.execute();
            }
        });
    }
    public static void SYSCS_SET_LOGGER_LEVEL_LOCAL(final String loggerName,final String logLevel) throws SQLException{
        Logging logging;
        try {
            logging = JMXUtils.getLocalMBeanProxy(JMXUtils.LOGGING_MANAGEMENT, Logging.class);
        }catch(MalformedObjectNameException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
        logging.setLoggerLevel(loggerName,logLevel);
    }

    public static void SYSCS_GET_LOGGER_LEVEL_LOCAL(final String loggerName,final ResultSet[] resultSet) throws SQLException{
        Logging logging;
        try {
            logging = JMXUtils.getLocalMBeanProxy(JMXUtils.LOGGING_MANAGEMENT, Logging.class);
        }catch(MalformedObjectNameException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

        String loggerLevel = logging.getLoggerLevel(loggerName);

        ResultHelper res = new ResultHelper();
        ResultHelper.VarcharColumn col = res.addVarchar("LOGLEVEL", 15);
        res.newRow();
        col.set(loggerLevel);

        resultSet[0] = res.getResultSet();
    }

    @SuppressFBWarnings("IIL_PREPARE_STATEMENT_IN_LOOP") // intentional (different servers)
    public static void SYSCS_GET_LOGGER_LEVEL(final String loggerName,final ResultSet[] resultSet) throws SQLException{
        List<String> loggerLevels = new ArrayList<>();

        executeOnAllServers( (hostAndPort, connection) -> {
            try (PreparedStatement ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_GET_LOGGER_LEVEL_LOCAL(?)")) {
                ps.setString(1, loggerName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        loggerLevels.add(rs.getString(1));
                    }
                }
            };
        });

        ResultHelper res = new ResultHelper();
        ResultHelper.VarcharColumn col = res.addVarchar("LOG_LEVEL", 120);

        List<ExecRow> rows = new ArrayList<>();
        for (String logger : loggerLevels) {
            res.newRow();
            col.set(logger);
        }

        resultSet[0] = res.getResultSet();
    }


    public static void SYSCS_GET_LOGGERS_LOCAL(final ResultSet[] resultSet) throws SQLException {
        Logging logging;
        try {
            logging = JMXUtils.getLocalMXBeanProxy(JMXUtils.LOGGING_MANAGEMENT, Logging.class);
        }catch(MalformedObjectNameException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

        ResultHelper res = new ResultHelper();
        ResultHelper.VarcharColumn col = res.addVarchar("SPLICELOGGER", 100);

        HashSet<String> loggerNames = new HashSet<>();
        loggerNames.addAll(logging.getLoggerNames());
        ArrayList<String> loggers = new ArrayList<>(loggerNames);
        Collections.sort(loggers);
        for(String logger : loggers){
            res.newRow();
            col.set(logger);
        }

        resultSet[0] = res.getResultSet();

    }

    public static void SYSCS_GET_LOGGERS(final ResultSet[] resultSet) throws SQLException{
        Set<String> loggers = new HashSet<>();

        executeOnAllServers( "call SYSCS_UTIL.SYSCS_GET_LOGGERS_LOCAL()", (hostAndPort, connection, rs) -> {
            while (rs.next()) {
                loggers.add(rs.getString(1));
            }
        });

        ResultHelper res = new ResultHelper();
        ResultHelper.VarcharColumn col = res.addVarchar("SPLICE_LOGGER", 100);

        ArrayList<String> loggerNames = new ArrayList<>(loggers);
        Collections.sort(loggerNames);

        List<ExecRow> rows = new ArrayList<>();
        for (String logger : loggerNames) {
            res.newRow();
            col.set(logger);
        }

        resultSet[0] = res.getResultSet();
    }
}
