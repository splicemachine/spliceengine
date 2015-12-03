package com.splicemachine.derby.hbase;

import com.splicemachine.db.drda.NetworkServerControl;
import com.splicemachine.derby.impl.job.scheduler.TieredTaskScheduler;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.si.impl.TransactionTimestamps;
import com.yammer.metrics.reporting.JmxReporter;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * This is the task that runs once when the entire region server *process* is shutting down -- that is, the JVM will
 * be exiting.  We try to shut down as gracefully as possible.  In any case don't leave any non-daemon threads
 * running which will prevent the JVM from exiting.
 * <p/>
 * The name of this class reflects that it is currently invoked from SpliceDriver.  It should be independent of
 * SpliceDriver, but SpliceDriver currently hold a state field which makes ensuring that only a single instance
 * of this task runs easy/convenient.
 */
class SpliceDriverShutdownRunnable implements Runnable {

    private static final Logger LOG = Logger.getLogger(SpliceDriverShutdownRunnable.class);

    private final JmxReporter metricsReporter;
    private final NetworkServerControl server;
    private final List<Service> services;
    private final TieredTaskScheduler threadTaskScheduler;

    public SpliceDriverShutdownRunnable(JmxReporter metricsReporter, NetworkServerControl server,
                                        List<Service> services, TieredTaskScheduler threadTaskScheduler) {
        this.metricsReporter = metricsReporter;
        this.server = server;
        this.services = services;
        this.threadTaskScheduler = threadTaskScheduler;
    }

    @Override
    public void run() {

        /**
         * Stop accepting new JDBC connections.
         */
        shutDown("JDBC connections", new ShutdownTask() {
            @Override
            public void run() throws Exception {
                if(server != null) {
                    server.shutdown();
                }
            }
        });

        /**
         * Shutdown existing task framework task scheduler.
         */
        shutDown("task framework threads", new ShutdownTask() {
            @Override
            public void run() {
                threadTaskScheduler.shutdown();
            }
        });

        /**
         * Stop anything that registered as a service with SpliceDriver
         */
        shutDown("services", new ShutdownTask() {
            public void run() {
                for (Service service : services) {
                    service.shutdown();
                }
            }
        });

        /**
         * Stop JMX
         */
        shutDown("JMX", new ShutdownTask() {
            @Override
            public void run() throws Exception {
                if (metricsReporter != null) {
                    metricsReporter.shutdown();
                }
            }
        });

        /**
         * Shutdown the Timestamp client (the server is shutdown on the master node).
         */
        shutDown("TimestampSource", new ShutdownTask() {
            @Override
            public void run() throws Exception {
                TransactionTimestamps.getTimestampSource().shutdown();
            }
        });
    }

    /**
     * Execute each shutdown task in isolation so that if one throws it won't prevent subsequent
     * shutdown tasks from running.
     */
    private static void shutDown(String description, ShutdownTask shutdownTask) {
        try {
            LOG.warn("SHUTTING DOWN: " + description);
            shutdownTask.run();
        } catch (Throwable t) {
            LOG.warn(String.format("Caught exception while running shutdown step '%s'", description), t);
        }
    }

    private interface ShutdownTask {
        void run() throws Exception;
    }

}
