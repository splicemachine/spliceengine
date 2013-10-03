package com.splicemachine.test.diff;

import com.splicemachine.test.verify.VerifyReport;
import difflib.Chunk;
import difflib.Delta;
import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;
import java.util.*;

/**
 * A report of the differences in the output of the execution of a given
 * SQL script against both Derby and Splice.
 */
public class DiffReport implements VerifyReport {
    public final String sqlFileName;
    public final String derbyFile;
    public final String spliceFile;
    public final List<Delta<String>> deltas;

    /**
     * Constructor
     * @param derbyFile derby output file
     * @param spliceFile splice output file
     * @param deltas the differences between the two
     */
    public DiffReport(String derbyFile, String spliceFile, List<Delta<String>> deltas) {
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
    @Override
    public String getScriptFileName() {
        return this.sqlFileName;
    }

    /**
     * Does this report have differences?
     * @return <code>true</code>, if so
     */
    @Override
    public boolean hasErrors() {
        return ! this.deltas.isEmpty();
    }

    /**
     * Get the number of differences in the output
     * @return number of differences
     */
    @Override
    public int getNumberOfErrors() {
        return (this.deltas.isEmpty() ? 0 : this.deltas.size());
    }

    /**
     * Pretty print the difference output of this report
     * @param out the output location
     */
    @Override
    public void print(PrintStream out) {
        if (out == null) {
            return;
        }
        out.println("\n===========================================================================================");
        out.println(sqlFileName);
        if (! hasErrors()) {
            out.println("No differences");
        } else {
            out.println(getNumberOfErrors()+" differences");
            printDeltas(this.deltas, out);
        }
        out.println("===========================================================================================");
        out.flush();
    }

    private void printDeltas(List<Delta<String>> deltas, PrintStream out) {
        for (Delta<String> delta: deltas) {
            out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            printChunk(this.derbyFile, delta.getOriginal(), out);
            out.println("++++++++++++++++++++++++++");
            printChunk(this.spliceFile, delta.getRevised(), out);
            out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }
    }

    private static void printChunk(String testType, Chunk<String> chunk, PrintStream out) {
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
