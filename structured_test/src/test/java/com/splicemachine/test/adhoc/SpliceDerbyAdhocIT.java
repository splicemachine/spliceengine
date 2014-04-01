package com.splicemachine.test.adhoc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.runner.DerbyRunner;
import com.splicemachine.test.runner.SpliceRunner;
import com.splicemachine.test.utils.TestUtils;
import com.splicemachine.test.verify.Verifier;
import com.splicemachine.test.verify.VerifyReport;

/**
 * Run an arbitrary SQL script (or scripts) against Derby and Splice and difference the output.
 * <p>
 *     Checks the "adhoc" directory for files with extension of ".sql" and runs them against Derby and Splice.<br/>
 *     <b>NOTE</b>: Each script should create any schema it needs, load data and drop the schema when finished.
 * </p>
 * @author Jeff Cunningham
 *         Date: 4/1/14
 */
public class SpliceDerbyAdhocIT {
    private static List<File> testFiles;

    private static DerbyRunner derbyRunner;
    private static SpliceRunner spliceRunner;

    private static boolean clean = true;

    @BeforeClass
    public static void beforeClass() throws Exception {

        // Gather the sql files we want to run as tests
        testFiles = TestUtils.getTestFileList(TestUtils.AD_HOC_DIR);

        derbyRunner = new DerbyRunner(TestUtils.TARGET_AD_HOC_DIR);
        spliceRunner = new SpliceRunner(TestUtils.TARGET_AD_HOC_DIR, "1527");
    }

    @Test(timeout=1000*60*5)  // Time out after 5 min
    public void runAdhocTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // run the tests
        TestUtils.runTests(testFiles, Arrays.asList(derbyRunner, spliceRunner), ps);

        // diff output and assert no differences in each report
        Verifier theDiffer = new DiffEngine(TestUtils.getBaseDirectory()+TestUtils.TARGET_AD_HOC_DIR,
                                            null, null);
        Collection<VerifyReport> reports = theDiffer.verifyOutput(testFiles);

        // report test output
        Map<String,Integer> failedDiffs = VerifyReport.Report.reportCollection(reports, ps);

        // write report to file
        String report = baos.toString("UTF-8");
        TestUtils.createLog(TestUtils.getBaseDirectory(), "SpliceDerbyAdhocIT.log", null, report, true, false);

        // make test assertion
        Assert.assertEquals(failedDiffs.size() + " tests had differences: " + failedDiffs.keySet() + "\n" + report,
                            reports.size(), (reports.size() - failedDiffs.size()));
    }

}
