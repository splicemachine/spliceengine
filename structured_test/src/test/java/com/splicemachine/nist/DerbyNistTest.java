package com.splicemachine.nist;

import java.io.File;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.DerbyEmbedConnection;

public class DerbyNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	public static final String TYPE = ".derby";
	protected static Connection connection;

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

        loadSchemaList(getResourceDirectory() + "/nist/schema.list", "#");
        loadSkipTests(getResourceDirectory() + "/nist/skip.tests", "#");
        nonSqlFilesFilter = Lists.newArrayList(BaseNistTest.SKIP_TESTS);
        nonSqlFilesFilter.addAll(BaseNistTest.SCHEMA_SCRIPTS);
	}

    @Test
	public void generateDerby() throws Exception {
		Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(SCHEMA_SCRIPTS,null),null);
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

		Collection<File> regularFiles = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null, nonSqlFilesFilter),null);
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