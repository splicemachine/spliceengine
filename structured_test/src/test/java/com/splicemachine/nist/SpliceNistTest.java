package com.splicemachine.nist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.SpliceNetConnection;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

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
		Collection<File> files = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(SCHEMA_SCRIPTS,null),null);
		for (File file: files) {
			executor.submit(new SpliceCallable(file));	
		}		
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
        executor = Executors.newFixedThreadPool(4);
		files = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(null,SKIP_TESTS),null);
		for (File file: files) {
			executor.submit(new SpliceCallable(file));	
		}		
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
		files = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(null,SKIP_TESTS),null);
		for (File file: files) {
			Patch patch = DiffUtils.diff(fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", DerbyNistTest.TYPE)), 
					fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", SpliceNistTest.TYPE)));
			 for (Object delta: patch.getDeltas()) {
                 System.out.println((Delta) delta);
			 }
		}

	}
	
	@Test
	public void chicken () {
		Collection<File> files = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(null,SKIP_TESTS),null);
		for (File file: files) {
			Patch patch = DiffUtils.diff(fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", DerbyNistTest.TYPE)), 
					fileToLines(getBaseDirectory()+"/target/nist/"+file.getName().replace(".sql", SpliceNistTest.TYPE)));
			 for (Object delta: patch.getDeltas()) {
				 System.out.println("File: " + file.getName());
                 System.out.println("        " + (Delta) delta);
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
	
	   private List<String> fileToLines(String filename) {
           List<String> lines = new LinkedList<String>();
           String line = "";
           try {
                   BufferedReader in = new BufferedReader(new FileReader(filename));
                   line = in.readLine();
                   while ((line = in.readLine()) != null) {
                           lines.add(line);
                   }
           } catch (IOException e) {
                   e.printStackTrace();
           }
           return lines;
   }
	
}
