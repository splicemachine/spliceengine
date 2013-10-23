package com.splicemachine.test.runner;

import com.splicemachine.test.connection.SpliceNetConnection;
import com.splicemachine.test.utils.TestUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test runner for Splice.
 * <p>
 *     An instance of this class runs all SQL scripts against a Splice server.
 * </p>
 * <p>
 *     This class was originally written to be a multi-threaded test engine.<br/>
 *     It started {@link TestUtils#DEFAULT_THREAD_POOL_SIZE} <code>Callable&lt;V&gt;</code>
 *     instances, each with a SQL script to run, and executed them against Splice.<br/>
 *     But since the SQL scripts are interdependent (some expect schema, tables created
 *     by others) and since multi-threaded clients execute in an indeterminate order,
 *     some SQL scripts were executing before schema was created for them.<br/>
 *     This runner is now executed in single-threaded mode.
 * </p>
 * @see DerbyRunner
 */
public class SpliceRunner implements TestRunner {
    private static final Logger LOG = Logger.getLogger(SpliceRunner.class);

    private final String outputDirectory;
    private final ExecutorService executor;
    private final String port;

    /**
     * Constructor. Initializes.
     * @param outputDirectory directory in which to place test output
     * @throws Exception
     */
    public SpliceRunner(String outputDirectory) throws Exception {
        this(outputDirectory,null);
    }

    /**
     * Constructor. Initializes.
     * @param outputDirectory directory in which to place test output
     * @param port optional port to connect to
     * @throws Exception
     */
    public SpliceRunner(String outputDirectory, String port) throws Exception {
    	this.outputDirectory = outputDirectory;
//        executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        executor = Executors.newSingleThreadExecutor();
        this.port = port;
        Connection connection = getConnection();
        connection.close();
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Run the given set of tests.
     * @param testFiles the SQL scripts to run
     * @throws Exception any failure
     */
    @Override
    public void run(List<File> testFiles) throws Exception {
        Collection<Future<String>> testRuns = new ArrayList<Future<String>>(testFiles.size());
        Connection connection = getConnection();
        for (File file: testFiles) {
            testRuns.add(executor.submit(new SpliceCallable(file, outputDirectory, connection)));
            if (connection.isClosed()) {
                // FIXME: this does not handle reconnect when "disconnect;"
                // is called from a script...
                LOG.warn("DB connection was closed. Attempting to get new...");
                connection = getConnection();
            }
        }

        for (Future<String> testRun : testRuns) {
            testRun.get();
        }
    }

    /**
     * Get a connection to this store
     * @return valid Connection
     * @throws Exception if an error occurs acquiring the connection
     */
    @Override
    public Connection getConnection() throws Exception {
    	return SpliceNetConnection.getConnection(this.port);
    }

    /**
     * The <code>Callable&lt;V&gt;</code>, instances of which run a
     * given SQL script.
     */
    public static class SpliceCallable implements Callable<String> {
		private final File file;
		private final String outputDirectory;
        private final Connection connection;

		public SpliceCallable(File file, String outputDirectory, Connection connection) {
			this.file = file;
			this.outputDirectory = outputDirectory;
            this.connection = connection;
		}

		@Override
		public String call() throws Exception {
			TestUtils.runTest(file,TestUtils.SPLICE_OUTPUT_EXT, outputDirectory, connection);
			return file.getName();
		}

	}
}
