package com.splicemachine.test.diff;

import com.splicemachine.test.nist.NistTestUtils;
import difflib.DiffUtils;
import difflib.Patch;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.splicemachine.test.nist.NistTestUtils.fileToLines;
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
        File sqlFile = new File(NistTestUtils.getResourceDirectory()+ NistTestUtils.TARGET_NIST_DIR, sqlFileName);
        List<File> files = Arrays.asList(sqlFile);

        List<DiffReport> reports = DiffEngine.diffOutput(files,
                NistTestUtils.getResourceDirectory() + "/difftest/", NistTestUtils.readDerbyFilters(), NistTestUtils.readSpliceFilters());
        Map<String,Integer> failedDiffs = DiffReport.reportCollection(reports, null); // System.out);

        int totalDiffs = 0;
        for (Map.Entry<String,Integer> entry : failedDiffs.entrySet()) {
            totalDiffs += entry.getValue();
        }

        Assert.assertEquals(expectedFailures, totalDiffs);
    }

    @Test
    public void testRawDiff() throws Exception {
        String derbyFileName = NistTestUtils.getResourceDirectory() + "/difftest/cdr002.derby";
        List<String> derbyFileLines = fileToLines(derbyFileName, (String)null);

        String spliceFileName = NistTestUtils.getResourceDirectory() + "/difftest/cdr002.splice";
        List<String> spliceFileLines = fileToLines(spliceFileName, (String)null);

        // using diff util directly here (no filtering), so expecting diffs
        Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        DiffReport report = new DiffReport(derbyFileName, spliceFileName, patch.getDeltas());
        PrintStream out = System.out;
//        report.print(out);
        Assert.assertTrue(report.hasDifferences());
    }

    @Test
    public void testRawDiffInDepth() throws Exception {
        // derby output
        String derbyFileName = NistTestUtils.getResourceDirectory() + "/difftest/fakeDiff01.derby";
        List<String> derbyFileLines = fileToLines(derbyFileName, (String)null);
        // filter derby warnings, etc
        derbyFileLines = DiffEngine.filterOutput(derbyFileLines, readDerbyFilters());

        // splice output
        String spliceFileName = NistTestUtils.getResourceDirectory() + "/difftest/fakeDiff01.splice";
        List<String> spliceFileLines = fileToLines(spliceFileName, (String)null);
        // filter splice warnings, etc
        spliceFileLines = DiffEngine.filterOutput(spliceFileLines, readSpliceFilters());

        // using diff util directly here (no filtering), so expecting diffs
        Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        DiffReport report = new DiffReport(derbyFileName, spliceFileName, patch.getDeltas());
//        report.print(System.out);
        Assert.assertTrue(report.hasDifferences());
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
        helpDiffOutputFiles("dml024.sql", 0);
    }

    @Test
    public void testDiff_dml079() throws Exception {
        //  Result column and NULL result ordering
        helpDiffOutputFiles("dml079.sql", 0);
    }

}
