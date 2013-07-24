package com.splicemachine.nist;

import com.splicemachine.derby.nist.DerbyEmbedConnection;
import com.splicemachine.derby.nist.SimpleConnectionPool;
import com.splicemachine.derby.nist.SpliceNetConnection;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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

    private static DerbyNistTest derbyTest;
    private static SpliceNistTest spliceTest;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Gather the sql files we want to run as tests
        testFiles = BaseNistTest.getTestFileList();
        // Read in the bug filters for output files
        derbyFilter = BaseNistTest.readDerbyFilters();
        spliceFilter = BaseNistTest.readSpliceFilters();

        spliceTest = new SpliceNistTest(new SimpleConnectionPool(new SpliceNetConnection()));
        derbyTest = new DerbyNistTest(new SimpleConnectionPool(new DerbyEmbedConnection()));
    }

    @Test(timeout=1000*60*360)  // Time out after 6 min
    public void runNistTest() throws Exception {
        runTests(testFiles);
    }

    @Test
    public void runTests() throws Exception {
        // need two tests run cause 2nd depends on schema creation in 1st
        List<File> tests = new ArrayList<File>();
        tests.add(new File(BaseNistTest.getResourceDirectory(), "/nist/schema8.sql"));
        tests.add(new File(BaseNistTest.getResourceDirectory(), "/nist/cdr002.sql"));
        runTests(tests);
    }

    private Collection<BaseNistTest.DiffReport> runTests(List<File> testFiles) throws Exception {
        // Assuming setup() has been called on both derby and splice ...
        System.out.println("Starting...");

        // run derby
        System.out.println("Derby...");

        System.out.println("    Running "+testFiles.size()+" tests...");
        long start = System.currentTimeMillis();
        derbyTest.runDerby(testFiles);
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // run splice
        System.out.println("Splice...");

        System.out.println("    Running "+testFiles.size()+" tests...");
        start = System.currentTimeMillis();
        spliceTest.runSplice(testFiles);
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // diff output and assert no differences in each report
        Collection<BaseNistTest.DiffReport> reports = BaseNistTest.diffOutput(testFiles, derbyFilter, spliceFilter);
        BaseNistTest.assertNoDiffs(reports);

        return reports;
    }
}
