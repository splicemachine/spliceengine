package com.splicemachine.client;

import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jleach on 4/11/17.
 */
public class SpliceClient {
    private static final Logger LOG = Logger.getLogger(SpliceClient.class);

    private static volatile boolean isClient = false;
    public static boolean isRegionServer = false;
    public static volatile String connectionString;
    public static volatile byte[] token;

    private static ScheduledExecutorService service = Executors.newScheduledThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("SpliceTokenRenewer").setDaemon(true).build());

    public static synchronized void setClient() {
        if (!isClient) {
            isClient = true;
            initializeTokenInternal();
        }
    }

    public static boolean isClient() {
        return isClient;
    }

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

            service.schedule(new Runnable() {
                @Override
                public void run() {
                    initializeTokenInternal();
                }
            }, 10, TimeUnit.SECONDS);
        }
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
}
