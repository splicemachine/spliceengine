package com.splicemachine.test.diff;

import com.splicemachine.test.nist.NistTestUtils;
import difflib.DiffUtils;
import difflib.Patch;
import difflib.myers.Equalizer;
import difflib.myers.MyersDiff;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 7/24/13
 */
public class DiffEngine {

    public static List<DiffReport> diffOutput(List<File> sqlFiles,
                                              String testOutputDir,
                                              List<String> derbyFilter,
                                              List<String> spliceFilter) {

        List<DiffReport> diffs = new ArrayList<DiffReport>();

        String inputDir = NistTestUtils.getBaseDirectory() + NistTestUtils.TARGET_NIST;
        if (testOutputDir != null && ! testOutputDir.isEmpty()) {
            inputDir = testOutputDir;
        }
        for (File sqlFile: sqlFiles) {
            // derby output
            String derbyFileName = inputDir + sqlFile.getName().replace(".sql", NistTestUtils.DERBY_OUTPUT_EXT);
            List<String> derbyFileLines = NistTestUtils.fileToLines(derbyFileName, "--", "ij> --");
            // filter derby warnings, etc
            derbyFileLines = filterOutput(derbyFileLines, derbyFilter);

            // splice output
            String spliceFileName = inputDir + sqlFile.getName().replace(".sql", NistTestUtils.SPLICE_OUTPUT_EXT);
            List<String> spliceFileLines = NistTestUtils.fileToLines(spliceFileName, "--", "ij> --");
            // filter splice warnings, etc
            spliceFileLines = filterOutput(spliceFileLines, spliceFilter);

            Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines, new DashedLineExqualizer());

            DiffReport diff = new DiffReport(derbyFileName, spliceFileName, patch.getDeltas());
            diffs.add(diff);
        }
        return diffs;
    }

    public static List<String> filterOutput(List<String> fileLines, List<String> warnings) {
        if (fileLines == null || fileLines.isEmpty()) {
            return fileLines;
        }
        List<String> copy = Collections.synchronizedList(new ArrayList<String>(fileLines));
        List<String> filteredLines = Collections.synchronizedList(new ArrayList<String>(fileLines.size()));
        for (String line : copy) {
            boolean filter = false;

            if (line.startsWith("CONNECTION")) {
                continue;
            }

            for (String warning : warnings) {
                if (line.contains(warning)) {
                    filter = true;
                    break;
                }
            }
            if (! filter) {
                filteredLines.add(line);
            }
        }
        return filteredLines;
    }

    private static class DashedLineExqualizer implements Equalizer<String> {

        @Override
        public boolean equals(String original, String revised) {
            // TODO: replace with regex for whole line contains '-'
            // FIXME: didn't work
            if (original.startsWith("-") && original.endsWith("-") &&
                    revised.startsWith("-") && revised.endsWith("-")) {
                return true;
            } else {
                return original.equals(revised);
            }
        }
    }
}
