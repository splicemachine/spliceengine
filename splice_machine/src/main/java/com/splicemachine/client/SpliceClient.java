package com.splicemachine.client;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.jdbc.ClientDriver;
import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jleach on 4/11/17.
 */
public class SpliceClient {
    private static final Logger LOG = Logger.getLogger(SpliceClient.class);

    public enum Mode {
        MASTER,
        EXECUTOR
    }

    private static volatile boolean isClient = false;
    @SuppressFBWarnings(value = "MS_SHOULD_BE_FINAL", justification = "DB-9844")
    public static boolean isRegionServer = false;
    public static volatile String connectionString;
    public static volatile byte[] token;

    private static ScheduledExecutorService service = Executors.newScheduledThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("SpliceTokenRenewer").setDaemon(true).build());

    public static synchronized void setClient(boolean tokenEnabled, Mode mode) {
        if (!isClient) {
            isClient = true;
            if (tokenEnabled) {
                if (mode.equals(Mode.MASTER))
                    grantHBasePrivileges();
                initializeTokenInternal();
            }
        }
    }

    @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION", justification = "DB-9844")
    private static void grantHBasePrivileges() {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (PreparedStatement statement = conn.prepareStatement("call SYSCS_UTIL.SYSCS_HBASE_OPERATION(?,?,?)")) {
                statement.setString(1, "splice:SPLICE_TXN"); // not used, reference any splice system table
                statement.setString(2, "grant");
                String userName = UserGroupInformation.getCurrentUser().getShortUserName();
                statement.setBlob(3, new ArrayInputStream(Bytes.toBytes(userName)));
                ResultSet rs = statement.executeQuery();
                rs.next();

                LOG.info("Granted HBase privileges on splice namespace to user " + userName);
            }
        } catch (Throwable t) {
            LOG.error("Error while granting HBase privileges, job might fail", t);
        }
    }

    public static boolean isClient() {
        return isClient;
    }

    @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION", justification = "DB-9844")
    private static synchronized void initializeTokenInternal() {
        byte[] oldToken = token;
        long cancellationWait;
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (PreparedStatement statement = conn.prepareStatement("call SYSCS_UTIL.SYSCS_GET_SPLICE_TOKEN(?)")) {
                statement.setString(1, UserGroupInformation.getCurrentUser().getShortUserName());
                ResultSet rs = statement.executeQuery();
                rs.next();
                token = rs.getBytes(1);
                Timestamp expiration = rs.getTimestamp(2);

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                Timestamp ts = new Timestamp(calendar.getTimeInMillis());

                long difference = expiration.getTime() - ts.getTime();
                long wait = (long) (difference * 0.6);
                cancellationWait = (long) (difference * 0.4);

                LOG.info("Got token expiring on " + expiration.toLocalDateTime());
                LOG.info("Scheduling renewal in " + wait + " milliseconds");

                service.schedule(new Runnable() {
                    @Override
                    public void run() {
                        initializeTokenInternal();
                    }
                }, wait, TimeUnit.MILLISECONDS);

            }
            if (oldToken == null)
                return;

            service.schedule(new Runnable() {
                @Override
                public void run() {
                    cancelToken(oldToken);
                }
            }, cancellationWait, TimeUnit.MILLISECONDS);

        } catch (Throwable t) {
            LOG.error("Error while getting Splice token", t);
            if (t instanceof SQLNonTransientConnectionException) {
                // This is a non-recoverable failure, so stop
                // with no retries.
                String message = t.getMessage();
                if (message.contains("userid or password invalid"))
                    throw new RuntimeException(t);
            }
            service.schedule(new Runnable() {
                @Override
                public void run() {
                    initializeTokenInternal();
                }
            }, 10, TimeUnit.SECONDS);
        }
    }

    static String parseJDBCPassword(String jdbcUrl) throws SqlException {
        Properties properties = ClientDriver.tokenizeURLProperties(jdbcUrl, null);
        return properties.getProperty("password");
    }

    private static void cancelToken(byte[] token) {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (PreparedStatement statement = conn.prepareStatement("call SYSCS_UTIL.SYSCS_CANCEL_SPLICE_TOKEN(?)")) {
                statement.setBlob(1, new ArrayInputStream(token));
                statement.execute();
            }
        } catch (Throwable t) {
            LOG.error("Error while cancelling old Splice token", t);
        }
    }

    private static volatile ComboPooledDataSource pool;
    @SuppressFBWarnings(value = "DC_PARTIALLY_CONSTRUCTED", justification = "DB-9844")
    public static DataSource getConnectionPool(boolean debugConnections, int maxConnections) {
        if (pool == null) {
            synchronized (SpliceClient.class) {
                if (pool == null) {
                    pool = new ComboPooledDataSource();
                    try {
                        String poolConnectionString = connectionString;
                        pool.setDriverClass("com.splicemachine.db.jdbc.Driver40");

                        // parse and mask password in JDBC url in order to make it not logged
                        String password = parseJDBCPassword(connectionString);
                        if (password != null) {
                            poolConnectionString = poolConnectionString.replace(
                                    "password="+password+";", "");
                            poolConnectionString = poolConnectionString.replace(
                                    "password="+password, "");
                        }
                        pool.setJdbcUrl(poolConnectionString);
                        if (password != null) {
                            pool.setPassword(password);
                        }

                        pool.setMinPoolSize(10);
                        pool.setAcquireIncrement(5);
                        pool.setMaxPoolSize(maxConnections);
                        if (debugConnections) {
                            pool.setUnreturnedConnectionTimeout(60);
                            pool.setDebugUnreturnedConnectionStackTraces(true);
                        }
                    } catch (PropertyVetoException e) {
                        e.printStackTrace();
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return pool;
    }
}
