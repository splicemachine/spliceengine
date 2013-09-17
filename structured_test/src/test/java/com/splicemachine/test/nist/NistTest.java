package com.splicemachine.test.nist;

import com.splicemachine.test.diff.DiffReport;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
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
public class NistTest {
    private static List<File> testFiles;
    private static List<String> derbyOutputFilter;
    private static List<String> spliceOutputFilter;

    private static DerbyNistRunner derbyRunner;
    private static SpliceNistRunner spliceRunner;

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

        derbyRunner = new DerbyNistRunner();
        spliceRunner = new SpliceNistRunner();
        
        if (clean) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
        	System.out.println("Cleaning before test...");
			NistTestUtils.cleanup(derbyRunner, spliceRunner, System.out);
		}
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if (clean) {
			// clean schema before test run for cleanup, unless noclean
        	// system property was specified
        	System.out.println("Cleaning after test...");
			NistTestUtils.cleanup(derbyRunner, spliceRunner, System.out);
		}
    }

    @Test(timeout=1000*60*12)  // Time out after 12 min
    public void runNistTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // run the tests
        Collection<DiffReport> reports = NistTestUtils.runTests(testFiles,
                                                                derbyRunner, derbyOutputFilter,
                                                                spliceRunner, spliceOutputFilter, ps);

        // report test output
        Map<String,Integer> failedDiffs = DiffReport.reportCollection(reports, ps);

        // write report to file
        String report = baos.toString("UTF-8");
        NistTestUtils.createLog(NistTestUtils.getBaseDirectory(), "NistTest.log", null, report);

        // make test assertion
        Assert.assertEquals(failedDiffs.size() + " tests had differences: " + failedDiffs.keySet() + "\n" + report,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
