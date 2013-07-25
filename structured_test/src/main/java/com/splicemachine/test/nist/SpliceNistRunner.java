package com.splicemachine.test.nist;

import com.splicemachine.test.connection.SpliceNetConnection;

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
 *     It started {@link NistTestUtils#DEFAULT_THREAD_POOL_SIZE} <code>Callable&lt;V&gt;</code>
 *     instances, each with a SQL script to run, and executed them against Splice.<br/>
 *     But since the SQL scripts are interdependent (some expect schema, tables created
 *     by others) and since multi-threaded clients execute in an indeterminate order,
 *     some SQL scripts were executing before schema was created for them.<br/>
 *     This runner is now executed in single-threaded mode.
 * </p>
 * @see DerbyNistRunner
 */
public class SpliceNistRunner extends NistTestUtils {
	private final ExecutorService executor;

    /**
     * Constructor. Initializes.
     * @throws Exception
     */
    public SpliceNistRunner() throws Exception {
//        executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        executor = Executors.newSingleThreadExecutor();
        Connection connection = SpliceNetConnection.getConnection();
        connection.close();
    }

    /**
     * Run the given set of tests.
     * @param testFiles the SQL scrips to run
     * @throws Exception any failure
     */
    public void runSplice(List<File> testFiles) throws Exception {
        Collection<Future<String>> testRuns = new ArrayList<Future<String>>(testFiles.size());
        Connection connection = SpliceNetConnection.getConnection();
        for (File file: testFiles) {
            testRuns.add(executor.submit(new SpliceCallable(file, connection)));
        }

        for (Future<String> testRun : testRuns) {
            testRun.get();
        }
    }

    /**
     * The <code>Callable&lt;V&gt;</code>, instances of which run a
     * given SQL script.
     */
    public static class SpliceCallable implements Callable<String> {
		private final File file;
        private final Connection connection;

		public SpliceCallable(File file, Connection connection) {
			this.file = file;
            this.connection = connection;
		}

		@Override
		public String call() throws Exception {
			runTest(file,SPLICE_OUTPUT_EXT, connection);
			return file.getName();
		}

	}
}
