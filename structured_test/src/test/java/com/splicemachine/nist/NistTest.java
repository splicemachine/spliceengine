package com.splicemachine.nist;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 7/22/13
 */
public class NistTest {
    private static List<File> testFiles;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Gather the sql files we want to run as tests
        testFiles = BaseNistTest.getTestFileList();
    }

    @Test(timeout=1000*60*360)  // Time out after 6 min
    public void runNistTest() throws Exception {
        runTests(testFiles);
    }

    @Test
    public void runOneTest() throws Exception {
        // need two tests run cause 2nd depends on schema creation in 1st
        List<File> oneFile = new ArrayList<File>();
        oneFile.add(new File(BaseNistTest.getResourceDirectory(), "/nist/schema8.sql"));
        oneFile.add(new File(BaseNistTest.getResourceDirectory(), "/nist/cdr002.sql"));
        runTests(oneFile);
    }

    private Collection<BaseNistTest.DiffReport> runTests(List<File> testFiles) throws Exception {
        System.out.println("Starting...");

        // run derby
        System.out.println("Derby...");
//        DerbyNistTest.setup();
//        long start = System.currentTimeMillis();
//        DerbyNistTest.createSchema();
//        System.out.println("    Schema creation and load: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        System.out.println("    Running "+testFiles.size()+" tests...");
        long start = System.currentTimeMillis();
        DerbyNistTest.runDerby(testFiles);
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // run splice
        System.out.println("Splice...");
//        SpliceNistTest.setup();
//        start = System.currentTimeMillis();
//        SpliceNistTest.createSchema();
//        System.out.println("    Schema creation and load: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        System.out.println("    Running "+testFiles.size()+" tests...");
        start = System.currentTimeMillis();
        SpliceNistTest.runSplice(testFiles);
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // diff output and assert no differences in each report
        Collection<BaseNistTest.DiffReport> reports = BaseNistTest.diffOutput(testFiles, new ArrayList<String>());
        BaseNistTest.assertNoDiffs(reports);

        return reports;
    }
}
