package com.splicemachine.nist;

import java.io.File;
import java.sql.Connection;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.DerbyEmbedConnection;

public class DerbyNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	protected static final String TYPE = ".derby";
	protected static Connection connection;
	/* Add Scripts Here to Run First */

	static {
		System.setProperty("derby.system.home", getBaseDirectory()+"/target/derby");
	}
	
	@BeforeClass 
	public static void setup() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("derby-nist-generator").build();
        executor = Executors.newFixedThreadPool(4);
        File derbyDir = new File(getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);
		connection = DerbyEmbedConnection.getCreateConnection();
		connection.close();
	}
		
	@Test
	public void generateDerby() throws Exception {
		Collection<File> files = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(SCHEMA_SCRIPTS,null),null);
		Connection connection = DerbyEmbedConnection.getConnection();
		connection.setAutoCommit(true);
		for (File file: files) {
			runTest(file,TYPE, connection);
		}		
		try {
			connection.close();
		} catch (Exception e) {
			connection.commit();
			connection.close();
		}

		Collection<File> regularFiles = FileUtils.listFiles(new File("src/test/resources/nist"), new SpliceIOFileFilter(null,SKIP_TESTS),null);
		connection = DerbyEmbedConnection.getConnection();
		connection.setAutoCommit(false);
		for (File file: regularFiles) {
			runTest(file,TYPE,DerbyEmbedConnection.getConnection());
		}				
		try {
			connection.close();
		} catch (Exception e) {
			connection.commit();
			connection.close();
		}

	}

		
}