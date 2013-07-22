package com.splicemachine.nist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.SpliceNetConnection;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class SpliceNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	
	public static void setup() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("derby-nist-generator").build();
        executor = Executors.newFixedThreadPool(4);
	}
		
	public static void runSplice() throws Exception {
		Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(SCHEMA_FILES,null),null);
		for (File file: files) {
			executor.submit(new SpliceCallable(file));	
		}		
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
        executor = Executors.newFixedThreadPool(4);
		files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null, NON_TEST_FILES_TO_FILTER),null);
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
