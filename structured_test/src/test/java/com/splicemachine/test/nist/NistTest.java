package com.splicemachine.test.nist;

import com.splicemachine.test.connection.SimpleConnectionPool;
import com.splicemachine.test.connection.SpliceNetConnection;
import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.diff.DiffReport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 7/22/13
 */
public class NistTest {
    private static List<File> testFiles;
    private static List<String> derbyFilter;
    private static List<String> spliceFilter;

    private static DerbyNistRunnerUtils derbyTest;
    private static SpliceNistRunnerUtils spliceTest;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Gather the sql files we want to run as tests
        testFiles = NistTestUtils.getTestFileList();

        // Read in the bug filters for output files
        derbyFilter = NistTestUtils.readDerbyFilters();
        spliceFilter = NistTestUtils.readSpliceFilters();

        derbyTest = new DerbyNistRunnerUtils();
        spliceTest = new SpliceNistRunnerUtils(new SimpleConnectionPool(new SpliceNetConnection()));
    }

    @Test(timeout=1000*60*360)  // Time out after 6 min
    public void runNistTest() throws Exception {
        runTests(testFiles);
    }

    // Temporary - for framework testing
    @Test
    public void runTests() throws Exception {
        // need two tests run cause 2nd depends on schema creation in 1st
        List<File> tests = new ArrayList<File>();
        tests.add(new File(NistTestUtils.getResourceDirectory(), "/nist/schema8.sql"));
        tests.add(new File(NistTestUtils.getResourceDirectory(), "/nist/cdr002.sql"));
        runTests(tests);
    }

    private Collection<DiffReport> runTests(List<File> testFiles) throws Exception {
        // Assuming setup() has been called on both derby and splice ...
        System.out.println("Starting...");

        // run derby
        System.out.println("Derby...");

        System.out.println("    Running "+testFiles.size()+" tests...");
        long start = System.currentTimeMillis();
        derbyTest.runDerby(testFiles);
        System.out.println("    Tests: " + NistTestUtils.getDuration(start, System.currentTimeMillis()));

        // run splice
        System.out.println("Splice...");

        System.out.println("    Running "+testFiles.size()+" tests...");
        start = System.currentTimeMillis();
        spliceTest.runSplice(testFiles);
        System.out.println("    Tests: " + NistTestUtils.getDuration(start, System.currentTimeMillis()));

        // diff output and assert no differences in each report
        Collection<DiffReport> reports = DiffEngine.diffOutput(testFiles,
                NistTestUtils.getBaseDirectory() + NistTestUtils.TARGET_NIST, derbyFilter, spliceFilter);
        assertNoDiffs(reports);

        return reports;
    }


    public static void assertNoDiffs(Collection<DiffReport> reports) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        int success = 0;
        for (DiffReport report : reports) {
            report.print(ps);
            if (! report.isEmpty()) {
                success++;
            }
        }

        Assert.assertEquals("Some test comparison failed: " + baos.toString("UTF-8") + "\n" + reports.size() + " Tests had differencs.", 0, success);
    }

}
