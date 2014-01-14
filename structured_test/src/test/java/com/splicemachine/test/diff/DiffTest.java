package com.splicemachine.test.diff;

import com.splicemachine.test.nist.NistTestUtils;
import com.splicemachine.test.utils.TestUtils;
import com.splicemachine.test.verify.VerifyReport;
import difflib.DiffUtils;
import difflib.Patch;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.splicemachine.test.nist.NistTestUtils.readDerbyFilters;
import static com.splicemachine.test.nist.NistTestUtils.readSpliceFilters;

/**
 * Tests for file differencing.
 *
 * @author Jeff Cunningham
 *         Date: 7/26/13
 */
public class DiffTest {


    private static void helpDiffOutputFiles(String sqlFileName, int expectedFailures) throws Exception {
        File sqlFile = new File(TestUtils.getResourceDirectory()+ "/difftest/", sqlFileName);
        List<File> files = Arrays.asList(sqlFile);

        DiffEngine diff = new DiffEngine(TestUtils.getResourceDirectory()+ "/difftest/",
        		NistTestUtils.readDerbyFilters(),
        		NistTestUtils.readSpliceFilters());
        List<VerifyReport> reports = diff.verifyOutput(files);
        Map<String,Integer> failedDiffs = VerifyReport.Report.reportCollection(reports, null); // System.out);

        int totalDiffs = 0;
        for (Map.Entry<String,Integer> entry : failedDiffs.entrySet()) {
            totalDiffs += entry.getValue();
        }

        Assert.assertEquals(getErrors(reports), expectedFailures, totalDiffs);
    }
    
    private static String getErrors(List<VerifyReport> reports) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        VerifyReport.Report.reportCollection(reports, ps);
        return baos.toString("UTF-8");
    }

    @Test
    public void testRawDiff() throws Exception {
        String derbyFileName = TestUtils.getResourceDirectory() + "/difftest/cdr002.derby";
        List<String> derbyFileLines = TestUtils.fileToLines(derbyFileName, null);

        String spliceFileName = TestUtils.getResourceDirectory() + "/difftest/cdr002.splice";
        List<String> spliceFileLines = TestUtils.fileToLines(spliceFileName, null);

        // using diff util directly here (no filtering), so expecting diffs
        Patch<String> patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        DiffReport report = new DiffReport(derbyFileName, spliceFileName, patch.getDeltas());
//        PrintStream out = System.out;
//        report.print(out);
        Assert.assertTrue(report.hasErrors());
    }

    @Test
    public void testRawDiffInDepth() throws Exception {
        // derby output
        String derbyFileName = TestUtils.getResourceDirectory() + "/difftest/fakeDiff01.derby";
        List<String> derbyFileLines = TestUtils.fileToLines(derbyFileName, null);
        // filter derby warnings, etc
        derbyFileLines = DiffEngine.filterOutput(derbyFileLines, readDerbyFilters());

        // splice output
        String spliceFileName = TestUtils.getResourceDirectory() + "/difftest/fakeDiff01.splice";
        List<String> spliceFileLines = TestUtils.fileToLines(spliceFileName, null);
        // filter splice warnings, etc
        spliceFileLines = DiffEngine.filterOutput(spliceFileLines, readSpliceFilters());

        // using diff util directly here (no filtering), so expecting diffs
        Patch<String> patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        DiffReport report = new DiffReport(derbyFileName, spliceFileName, patch.getDeltas());
//        report.print(System.out);
        Assert.assertTrue(report.hasErrors());
    }

    @Test
    public void testDiff_cdr002() throws Exception {
        helpDiffOutputFiles("cdr002.sql", 0);
    }

    @Test
    public void testDiff_schema8() throws Exception {
        // output file from loss of connection run
        // - expected diffs
        helpDiffOutputFiles("schema8.sql", 66);
    }

    @Test
    public void testDiff_dml005() throws Exception {
        helpDiffOutputFiles("dml005.sql", 0);
    }

    @Test
    public void testDiff_dml182() throws Exception {
        helpDiffOutputFiles("dml182.sql", 0);
    }

    @Test
    public void testDiff_dml059() throws Exception {
        // These output files are from when we had a bug
        // - expected diff
        helpDiffOutputFiles("dml059.sql", 4);
    }

    @Test
    public void testDiff_dml023() throws Exception {
        // NULL result ordering
        helpDiffOutputFiles("dml023.sql", 0);
    }

    @Test
    public void testDiff_dml024() throws Exception {
        // Result column ordering
        helpDiffOutputFiles("dml024.sql", 1);
    }

    @Test
    public void testDiff_dml079() throws Exception {
        //  Result column and NULL result ordering
        helpDiffOutputFiles("dml079.sql", 0);
    }

}
