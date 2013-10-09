package com.splicemachine.test.nist;

import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.runner.DerbyRunner;
import com.splicemachine.test.runner.SpliceRunner;
import com.splicemachine.test.utils.TestUtils;
import com.splicemachine.test.verify.Verifier;
import com.splicemachine.test.verify.VerifyReport;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.junit.Assert;

/**
 * Manually run all NIST SQL scripts (Splice server must be started manually).
 * Clean DB schema before and after unless <code>-Dnoclean</code> was specified
 *
 * @author Jeff Cunningham
 *         Date: 10/8/13
 */
public class TestNist {
    private static java.util.List<java.io.File> testFiles;
    private static java.util.List<String> derbyOutputFilter;
    private static java.util.List<String> spliceOutputFilter;

    private static com.splicemachine.test.runner.DerbyRunner derbyRunner;
    private static com.splicemachine.test.runner.SpliceRunner spliceRunner;

    private static boolean clean = true;

    @org.junit.BeforeClass
    public static void beforeClass() throws Exception {
    	String noClean = System.getProperty("noclean", null);
    	if (noClean != null) {
            clean = false;
        }

        // Gather the sql files we want to run as tests
        String singleScript = System.getProperty("script", null);
        if (singleScript == null) {
            testFiles = NistTestUtils.getTestFileList();
        } else {
            testFiles = NistTestUtils.createRunList(singleScript);
        }

        // Read in the bug filters for output files
        derbyOutputFilter = NistTestUtils.readDerbyFilters();
        spliceOutputFilter = NistTestUtils.readSpliceFilters();

        derbyRunner = new DerbyRunner(NistTestUtils.TARGET_NIST_DIR);
        spliceRunner = new SpliceRunner(NistTestUtils.TARGET_NIST_DIR);

        if (clean && derbyRunner != null && spliceRunner != null) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
        	System.out.println("Cleaning before test...");
        	ps.println("Cleaning before test...");

			NistTestUtils.cleanup(java.util.Arrays.asList(derbyRunner, spliceRunner), ps);

	        // write report to file
	        String report = baos.toString("UTF-8");
	        TestUtils.createLog(TestUtils.getBaseDirectory(), "Cleanup.log", null, report, true, false);
		}
    }

    @org.junit.AfterClass
    public static void afterClass() throws Exception {
        if (clean && derbyRunner != null && spliceRunner != null) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
        	System.out.println("Cleaning after test...");
        	ps.println("Cleaning after test...");

        	NistTestUtils.cleanup(java.util.Arrays.asList(derbyRunner, spliceRunner), ps);

	        // write report to file
	        String report = baos.toString("UTF-8");
	        TestUtils.createLog(TestUtils.getBaseDirectory(), "Cleanup.log", null, report, true, true);
		}
    }

    @org.junit.Test(timeout=1000*60*12)  // Time out after 12 min
    public void runNistTest() throws Exception {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.PrintStream ps = new java.io.PrintStream(baos);

        // run the tests
        TestUtils.runTests(testFiles, Arrays.asList(derbyRunner, spliceRunner), ps);

        // diff output and assert no differences in each report
        Verifier theDiffer = new DiffEngine(TestUtils.getBaseDirectory()+ NistTestUtils.TARGET_NIST_DIR,
        		derbyOutputFilter, spliceOutputFilter);
        Collection<VerifyReport> reports = theDiffer.verifyOutput(testFiles);

        // report test output (Map<name,# failures)
        Map<String,Integer> failedDiffs = VerifyReport.Report.reportCollection(reports, ps);

        // write report to file
        String report = baos.toString("UTF-8");
        TestUtils.createLog(TestUtils.getBaseDirectory(), "NistIT.log", null, report, true, false);

        // make test assertion
        Assert.assertEquals(failedDiffs.size() + " tests had differences: " + failedDiffs.keySet() + "\n" + report,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
