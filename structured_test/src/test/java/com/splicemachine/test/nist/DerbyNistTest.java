package com.splicemachine.test.nist;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.test.runner.DerbyRunner;
import com.splicemachine.test.runner.TestRunner;
import com.splicemachine.test.utils.TestUtils;

/**
 * Run all NIST SQL scripts through vanilla Derby.
 *
 * @author Jeff Cunningham
 *         Date: 7/22/13
 */
public class DerbyNistTest {
    private static List<File> testFiles;

    private static DerbyRunner derbyRunner;

    @BeforeClass
    public static void beforeClass() throws Exception {
    	testFiles = NistTestUtils.getTestFileList();

        derbyRunner = new DerbyRunner(NistTestUtils.TARGET_NIST_DIR);
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
    	// TODO: Clean derby DB unless -Dnoclean
    }

    @Test
    public void runTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // run the tests
        TestUtils.runTests(testFiles, Arrays.<TestRunner>asList(derbyRunner), ps);

        // write report to file
        String report = baos.toString("UTF-8");
        TestUtils.createLog(TestUtils.getBaseDirectory(), "DerbyTest.log", null, report, true, false);
    }
}
