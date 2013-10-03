package com.splicemachine.test.nist;

import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.runner.DerbyRunner;
import com.splicemachine.test.runner.SpliceRunner;
import com.splicemachine.test.utils.TestUtils;
import com.splicemachine.test.verify.Verifier;
import com.splicemachine.test.verify.VerifyReport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Run all NIST SQL scripts.  Clean schema before and after unless
 * -Dnoclean was specified
 *
 * @author Jeff Cunningham
 *         Date: 7/22/13
 */
public class NistIT {
    private static List<File> testFiles;
    private static List<String> derbyOutputFilter;
    private static List<String> spliceOutputFilter;

    private static DerbyRunner derbyRunner;
    private static SpliceRunner spliceRunner;

    private static boolean clean = true;

    @BeforeClass
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
        
        if (clean && spliceRunner != null) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
        	System.out.println("Cleaning before test...");
        	ps.println("Cleaning before test...");

			NistTestUtils.cleanup(Arrays.asList(derbyRunner, spliceRunner), ps);
			
	        // write report to file
	        String report = baos.toString("UTF-8");
	        TestUtils.createLog(TestUtils.getBaseDirectory(), "Cleanup.log", null, report, false);
		}
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if (clean && derbyRunner != null && spliceRunner != null) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
        	System.out.println("Cleaning after test...");
        	ps.println("Cleaning after test...");

        	NistTestUtils.cleanup(Arrays.asList(derbyRunner, spliceRunner), ps);
			
	        // write report to file
	        String report = baos.toString("UTF-8");
	        TestUtils.createLog(TestUtils.getBaseDirectory(), "Cleanup.log", null, report, true);
		}
    }

    @Test(timeout=1000*60*12)  // Time out after 12 min
    public void runNistTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // run the tests
        TestUtils.runTests(testFiles, Arrays.asList(derbyRunner, spliceRunner), ps);

        // diff output and assert no differences in each report
        Verifier theDiffer = new DiffEngine(TestUtils.getBaseDirectory()+NistTestUtils.TARGET_NIST_DIR,
        		derbyOutputFilter, spliceOutputFilter);
        Collection<VerifyReport> reports = theDiffer.verifyOutput(testFiles);

        // report test output
        Map<String,Integer> failedDiffs = VerifyReport.Report.reportCollection(reports, ps);

        // write report to file
        String report = baos.toString("UTF-8");
        TestUtils.createLog(TestUtils.getBaseDirectory(), "NistIT.log", null, report, false);

        // make test assertion
        Assert.assertEquals(failedDiffs.size() + " tests had differences: " + failedDiffs.keySet() + "\n" + report,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
