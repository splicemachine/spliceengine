package com.splicemachine.test.nist;

import com.splicemachine.test.connection.DerbyEmbedConnection;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.List;

/**
 * Test runner for Derby.
 * @see SpliceNistRunner
 */
public class DerbyNistRunner extends NistTestUtils {
    private static final Logger LOG = Logger.getLogger(DerbyNistRunner.class);

    static {
		System.setProperty("derby.system.home", NistTestUtils.getBaseDirectory()+"/target/derby");
	}

    /**
     * Constructor. Initializes by deleting a previous instance of the Derby DB
     * and creating a new embedded connection to Derby.
     * @throws Exception for failure to find
     */
    public DerbyNistRunner() throws Exception {
        File derbyDir = new File(NistTestUtils.getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(NistTestUtils.getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);
        Connection connection = getConnection();
        connection.close();
    }

    /**
     * Run the given set of tests.
     * @param testFiles the SQL scrips to run
     * @throws Exception any failure
     */
    public void runDerby(List<File> testFiles) throws Exception {

        Connection connection = getConnection();
        for (File file: testFiles) {
            NistTestUtils.runTest(file, NistTestUtils.DERBY_OUTPUT_EXT, connection);
            if (connection.isClosed()) {
                LOG.warn("DB connection was closed. Attempting to get new...");
                connection = getConnection();
            }
        }
        try {
            connection.close();
        } catch (Exception e) {
            connection.commit();
            connection.close();
        }
    }

    /**
     * Get a connection to this store
     * @return valid Connection
     * @throws Exception if an error occurs acquiring the connection
     */
    public Connection getConnection() throws Exception {
    	return DerbyEmbedConnection.getConnection();
    }
}