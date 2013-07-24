package com.splicemachine.test.diff;

import difflib.Chunk;
import difflib.Delta;
import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 7/24/13
 */
public class DiffReport {
    public final String derbyFile;
    public final List<Delta> deltas;
    public final String spliceFile;

    public DiffReport(String derbyFile, String spliceFile, List<Delta> deltas) {
        this.derbyFile = derbyFile;
        this.spliceFile = spliceFile;
        if (! deltas.isEmpty()) {
            this.deltas = deltas;
        } else {
            this.deltas = Collections.emptyList();
        }
    }

    public boolean isEmpty() {
        return this.deltas.isEmpty();
    }

    public int getNumberOfDiffs() {
        return (this.deltas.isEmpty() ? 0 : this.deltas.size());
    }

    public void print(PrintStream out) {
        out.println("\n===========================================================================================");
        out.println(sqlName(derbyFile));
        if (isEmpty()) {
            out.println("No differences");
        } else {
            out.println(getNumberOfDiffs()+" differences");
            printDeltas(this.deltas, out);
        }
        out.println("===========================================================================================");
        out.flush();
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
            out.println("  " + line);
        }
    }


    private static String sqlName(String fullName) {
        String[] cmpnts = StringUtils.split(fullName, '/');
        String baseName = cmpnts[cmpnts.length-1];
        baseName = baseName.replace(".derby",".sql");
        return baseName;
    }
}
