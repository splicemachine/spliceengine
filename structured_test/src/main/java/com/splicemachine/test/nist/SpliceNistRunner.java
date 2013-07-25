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

public class SpliceNistRunner extends NistTestUtils {
	private final ExecutorService executor;
	
	public SpliceNistRunner() throws Exception {
//        executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        executor = Executors.newSingleThreadExecutor();
        Connection connection = SpliceNetConnection.getConnection();
        connection.close();
    }

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
