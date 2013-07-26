package com.splicemachine.test.nist.test;

import com.splicemachine.test.diff.DiffReport;
import com.splicemachine.test.nist.DerbyNistRunner;
import com.splicemachine.test.nist.NistTest;
import com.splicemachine.test.nist.NistTestUtils;
import com.splicemachine.test.nist.SpliceNistRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TODO: Temporary - for framework testing
 * @author Jeff Cunningham
 *         Date: 7/25/13
 */
public class ShortNistTest {

    private static List<String> derbyOutputFilter;
    private static List<String> spliceOutputFilter;

    private static DerbyNistRunner derbyRunner;
    private static SpliceNistRunner spliceRunner;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Read in the bug filters for output files
        derbyOutputFilter = NistTestUtils.readDerbyFilters();
        spliceOutputFilter = NistTestUtils.readSpliceFilters();

        derbyRunner = new DerbyNistRunner();
        spliceRunner = new SpliceNistRunner();
    }

    @Test
    public void runTests() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // need two tests run cause 2nd depends on schema creation in 1st
        List<File> testFiles = new ArrayList<File>();
        testFiles.add(new File(NistTestUtils.getResourceDirectory(), "/nist/schema8.sql"));
        testFiles.add(new File(NistTestUtils.getResourceDirectory(), "/nist/cdr002.sql"));
        Collection<DiffReport> reports = NistTestUtils.runTests(testFiles, derbyRunner, derbyOutputFilter, spliceRunner, spliceOutputFilter, ps);

        Map<String,Integer> failedDiffs = DiffReport.reportCollection(reports, ps);

        System.out.print(baos.toString("UTF-8"));

        Assert.assertEquals(failedDiffs.size() +  " tests had differences: "+failedDiffs,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
}
