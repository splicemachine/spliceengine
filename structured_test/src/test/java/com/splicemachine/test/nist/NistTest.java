package com.splicemachine.test.nist;

import com.splicemachine.test.diff.DiffReport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

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

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Gather the sql files we want to run as tests
        testFiles = NistTestUtils.getTestFileList();

        // Read in the bug filters for output files
        derbyOutputFilter = NistTestUtils.readDerbyFilters();
        spliceOutputFilter = NistTestUtils.readSpliceFilters();

        derbyRunner = new DerbyNistRunner();
        spliceRunner = new SpliceNistRunner();
    }

    @Test(timeout=1000*60*360)  // Time out after 6 min
    public void runNistTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        Collection<DiffReport> reports = NistTestUtils.runTests(testFiles, derbyRunner, derbyOutputFilter, spliceRunner, spliceOutputFilter, ps);

        List<String> failedDiffs = DiffReport.reportCollection(reports, ps);

        System.out.print(baos.toString("UTF-8"));

        Assert.assertEquals(failedDiffs.size() +  " tests had differences: "+failedDiffs,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
