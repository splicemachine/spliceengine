package com.splicemachine.test.diff;

import difflib.Chunk;
import difflib.Delta;
import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;
import java.util.*;

/**
 * A report of the differences in the output of the execution of a given
 * SQL script against both Derby and Splice.
 */
public class DiffReport {
    public final String sqlFileName;
    public final String derbyFile;
    public final String spliceFile;
    public final List<Delta> deltas;

    /**
     * Constructor
     * @param derbyFile derby output file
     * @param spliceFile splice output file
     * @param deltas the differences between the two
     */
    public DiffReport(String derbyFile, String spliceFile, List<Delta> deltas) {
        this.derbyFile = derbyFile;
        this.spliceFile = spliceFile;
        if (! deltas.isEmpty()) {
            this.deltas = deltas;
        } else {
            this.deltas = Collections.emptyList();
        }
        this.sqlFileName = sqlName(this.derbyFile);
    }

    /**
     * Get the base name of the SQL script that was run to produce
     * this output
     * @return SQL script base name
     */
    public String getSqlFileName() {
        return this.sqlFileName;
    }

    /**
     * Does this report have differences?
     * @return <code>true</code>, if so
     */
    public boolean hasDifferences() {
        return ! this.deltas.isEmpty();
    }

    /**
     * Get the number of differences in the output
     * @return number of differences
     */
    public int getNumberOfDiffs() {
        return (this.deltas.isEmpty() ? 0 : this.deltas.size());
    }

    /**
     * Pretty print the difference output of this report
     * @param out the output location
     */
    public void print(PrintStream out) {
        out.println("\n===========================================================================================");
        out.println(sqlFileName);
        if (! hasDifferences()) {
            out.println("No differences");
        } else {
            out.println(getNumberOfDiffs()+" differences");
            printDeltas(this.deltas, out);
        }
        out.println("===========================================================================================");
        out.flush();
    }

    /**
     * Create a report of all difference reports in the collection.
     * @param reports the collection of difference reports
     * @param ps location of output
     * @return the mapping names of all scripts that have output file differences
     * and their number of differences
     */
    public static Map<String, Integer> reportCollection(Collection<DiffReport> reports, PrintStream ps) {
        Map<String, Integer> failedTestMap = new HashMap<String, Integer>();
        for (DiffReport report : reports) {
            report.print(ps);
            if (report.hasDifferences()) {
                failedTestMap.put(report.sqlFileName,report.getNumberOfDiffs());
            }
        }
        ps.println(reports.size()+" tests run. "+failedTestMap.size()+" failed output differencing.");
        return failedTestMap;
    }

    private void printDeltas(List<Delta> deltas, PrintStream out) {
        for (Delta delta: deltas) {
            out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            printChunk(this.derbyFile, delta.getOriginal(), out);
            out.println("++++++++++++++++++++++++++");
            printChunk(this.spliceFile, delta.getRevised(), out);
            out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }
    }

    private static void printChunk(String testType, Chunk chunk, PrintStream out) {
        out.println(testType + "\nPosition " + chunk.getPosition() + ": ");
        for(Object line : chunk.getLines()) {
            out.println("  [" + line + "]");
        }
    }

    private static String sqlName(String fullName) {
        String[] cmpnts = StringUtils.split(fullName, '/');
        String baseName = cmpnts[cmpnts.length-1];
        baseName = baseName.replace(".derby",".sql");
        return baseName;
    }
}
