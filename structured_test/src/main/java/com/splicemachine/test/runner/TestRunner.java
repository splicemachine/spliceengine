package com.splicemachine.test.runner;

import java.io.File;
import java.sql.Connection;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 10/3/13
 */
public interface TestRunner {

    /**
     * Run the given set of tests.
     * @param testFiles the SQL scripts to run
     * @throws Exception any failure
     */
    void run(List<File> testFiles) throws Exception;

    /**
     * Get a connection to this store
     * @return valid Connection
     * @throws Exception if an error occurs acquiring the connection
     */
    Connection getConnection() throws Exception;

    /**
     * Get this test runner's name
     * @return name or "type" of test runner implementation
     */
    String getName();
}
