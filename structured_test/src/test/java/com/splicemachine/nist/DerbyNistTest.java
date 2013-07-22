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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.nist.DerbyEmbedConnection;

public class DerbyNistTest extends BaseNistTest {
	protected static ExecutorService executor;
	protected static Connection connection;

    static {
		System.setProperty("derby.system.home", getBaseDirectory()+"/target/derby");
	}
	
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

        loadFilteredFiles();

        // create, load schema
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(SCHEMA_FILES,null),null);
        Connection connection = DerbyEmbedConnection.getConnection();
        connection.setAutoCommit(true);
        for (File file: files) {
            runTest(file, DERBY_OUTPUT_EXT, connection);
        }
        try {
            connection.close();
        } catch (Exception e) {
            connection.commit();
            connection.close();
            throw e;
        }
    }

    public static void runDerby() throws Exception {

        Collection<File> regularFiles = FileUtils.listFiles(new File(getResourceDirectory(),"/nist"), new SpliceIOFileFilter(null, NON_TEST_FILES_TO_FILTER),null);
        connection = DerbyEmbedConnection.getConnection();
        connection.setAutoCommit(false);
        for (File file: regularFiles) {
            runTest(file, DERBY_OUTPUT_EXT,DerbyEmbedConnection.getConnection());
        }
        try {
            connection.close();
        } catch (Exception e) {
            connection.commit();
            connection.close();
        }
    }

}