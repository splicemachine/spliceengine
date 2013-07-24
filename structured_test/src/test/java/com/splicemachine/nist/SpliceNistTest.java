package com.splicemachine.nist;

import com.splicemachine.derby.nist.ConnectionPool;
import com.splicemachine.derby.nist.SimpleConnectionPool;
import com.splicemachine.derby.nist.SpliceNetConnection;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SpliceNistTest extends BaseNistTest {
    private final ConnectionPool pool;
	private final ExecutorService executor;
	
	public SpliceNistTest(ConnectionPool pool) {
        this.pool = pool;
//        executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        executor = Executors.newSingleThreadExecutor();
    }

    public void runSplice(List<File> testFiles) throws Exception {
        Collection<Future<String>> testRuns = new ArrayList<Future<String>>(testFiles.size());
        for (File file: testFiles) {
            Connection connection = pool.getConnection();
            testRuns.add(executor.submit(new SpliceCallable(file, connection)));
            pool.returnConnection(connection);
        }

        for (Future<String> testRun : testRuns) {
            testRun.get();
        }
    }

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
