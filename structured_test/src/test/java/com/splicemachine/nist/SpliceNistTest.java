package com.splicemachine.nist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.SpliceNetConnection;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class SpliceNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	
	public static void setup() throws Exception {
        // TODO: Remove
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("derby-nist-generator").build();
    }

    public static void createSchema() throws Exception {
        // Load the tests
        runSplice(SCHEMA_FILES);
     }

    public static void runSplice() throws Exception {
        // Run the tests
        runSplice(NON_TEST_FILES_TO_FILTER);
    }

    public static void runSplice(List<String> filesToFilter) throws Exception {
        executor = Executors.newFixedThreadPool(4);
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null, filesToFilter),null);
        for (File file: files) {
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
