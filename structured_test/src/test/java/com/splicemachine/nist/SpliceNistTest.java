package com.splicemachine.nist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.SpliceNetConnection;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
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
	protected static final String TYPE = ".splice";
	protected static ExecutorService executor;
	
	@BeforeClass 
	public static void setup() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("derby-nist-generator").build();
        executor = Executors.newFixedThreadPool(4);
	}
		
	@Test
	public void generateSplice() throws Exception {
		Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(SCHEMA_SCRIPTS,null),null);
		for (File file: files) {
			executor.submit(new SpliceCallable(file));	
		}		
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
        executor = Executors.newFixedThreadPool(4);
		files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null,nonSqlFilesFilter),null);
		for (File file: files) {
			executor.submit(new SpliceCallable(file));	
		}		
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
		files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null,nonSqlFilesFilter),null);
		for (File file: files) {
			Patch patch = DiffUtils.diff(fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", DerbyNistTest.TYPE), null),
					fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", SpliceNistTest.TYPE), null));
			 for (Object delta: patch.getDeltas()) {
                 System.out.println((Delta) delta);
			 }
		}

	}
	
	@Test
	public void chicken () {
		Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null,nonSqlFilesFilter),null);
		for (File file: files) {
			Patch patch = DiffUtils.diff(fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", DerbyNistTest.TYPE), null),
					fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", SpliceNistTest.TYPE), null));
			 for (Object delta: patch.getDeltas()) {
				 System.out.println("File: " + file.getName());
                 System.out.println("        " + delta);
			 }
		}
	}

	public class SpliceCallable implements Callable<Void> {
		protected File file;
		public SpliceCallable(File file) {
			this.file = file;
		}

		@Override
		public Void call() throws Exception {
			Connection connection = SpliceNetConnection.getConnection();
			runTest(file,TYPE, connection);
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
