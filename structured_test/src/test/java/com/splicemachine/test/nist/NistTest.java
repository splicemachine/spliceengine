package com.splicemachine.test.nist;

import com.splicemachine.test.diff.DiffReport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Run all NIST SQL scripts
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

    private static boolean justDrop;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Gather the sql files we want to run as tests
        String singleScript = System.getProperty("script", null);
        if (singleScript == null) {
            testFiles = NistTestUtils.getTestFileList();
        } else if (singleScript.equalsIgnoreCase(NistTestUtils.DROP_SQL)) {
            justDrop = true;
        } else {
            testFiles = NistTestUtils.createRunList(singleScript);
        }

        // Read in the bug filters for output files
        derbyOutputFilter = NistTestUtils.readDerbyFilters();
        spliceOutputFilter = NistTestUtils.readSpliceFilters();

        derbyRunner = new DerbyNistRunner();
        spliceRunner = new SpliceNistRunner();
    }

    @Test(timeout=1000*60*12)  // Time out after 12 min
    public void runNistTest() throws Exception {
        if (justDrop) {
            NistTestUtils.runDrop(derbyRunner, spliceRunner);
            return;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        Collection<DiffReport> reports = NistTestUtils.runTests(testFiles,
                                                                derbyRunner, derbyOutputFilter,
                                                                spliceRunner, spliceOutputFilter, ps);

        Map<String,Integer> failedDiffs = DiffReport.reportCollection(reports, ps);

        String report = baos.toString("UTF-8");
        NistTestUtils.createLog(NistTestUtils.getBaseDirectory(), "NistTest.log", null, report);

        Assert.assertEquals(failedDiffs.size() + " tests had differences: " + failedDiffs + "\n" + report,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
