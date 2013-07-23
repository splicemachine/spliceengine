package com.splicemachine.nist;

import com.splicemachine.derby.nist.SpliceNetConnection;

import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SpliceNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	
	public static void setup() throws Exception {
        // nothing to do
    }

    public static void runSplice(List<File> testFiles) throws Exception {
        executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);

        for (File file: testFiles) {
            executor.submit(new SpliceCallable(file));
        }
        executor.shutdown();
        while (!executor.isTerminated()) {

        }
    }

	public static class SpliceCallable implements Callable<Void> {
		protected File file;
		public SpliceCallable(File file) {
			this.file = file;
		}

		@Override
		public Void call() throws Exception {
			Connection connection = SpliceNetConnection.getConnection();
			runTest(file,SPLICE_OUTPUT_EXT, connection);
			try {
				connection.close();
			} catch (Exception e) {
				connection.commit();
				connection.close();
			}
			return null;
		}
		
	}
}
