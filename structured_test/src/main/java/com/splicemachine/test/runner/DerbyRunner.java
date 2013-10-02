package com.splicemachine.test.runner;

import com.splicemachine.test.connection.DerbyEmbedConnection;
import com.splicemachine.test.utils.TestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.List;

/**
 * Test runner for Derby.
 * @see SpliceRunner
 */
public class DerbyRunner {
    private static final Logger LOG = Logger.getLogger(DerbyRunner.class);
    private static final String TARGET_DERBY_DIR = "/target/derby";

    static {
		System.setProperty("derby.system.home", TestUtils.getBaseDirectory()+TARGET_DERBY_DIR);
	}
    
    private final String outputDirectory;

    /**
     * Constructor. Initializes by deleting a previous instance of the Derby DB
     * and creating a new embedded connection to Derby.
     * @param outputDirectory directory in which to place test output
     * @throws Exception for failure to find
     */
    public DerbyRunner(String outputDirectory) throws Exception {
    	this.outputDirectory = outputDirectory;
        File derbyDir = new File(TestUtils.getBaseDirectory()+TARGET_DERBY_DIR);
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File outputDir = new File(TestUtils.getBaseDirectory()+this.outputDirectory);
        if (outputDir.exists())
        	FileUtils.deleteDirectory(outputDir);
        Connection connection = getConnection();
        connection.close();
    }

    /**
     * Run the given set of tests.
     * @param testFiles the SQL scripts to run
     * @throws Exception any failure
     */
    public void runDerby(List<File> testFiles) throws Exception {

        Connection connection = getConnection();
        for (File file: testFiles) {
            TestUtils.runTest(file, TestUtils.DERBY_OUTPUT_EXT, outputDirectory, connection);
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