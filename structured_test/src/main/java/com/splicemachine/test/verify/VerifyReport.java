package com.splicemachine.test.verify;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jeff Cunningham
 *         Date: 10/3/13
 */
public interface VerifyReport {

    /**
     * Get the base name of the SQL script that was run to produce
     * this output
     * @return SQL script base name
     */
    String getScriptFileName();

    /**
     * Does this report have errors?
     * @return <code>true</code>, if so
     */
    boolean hasErrors();

    /**
     * Get the number of errors in the output
     * @return number of errors
     */
    int getNumberOfErrors();

    /**
     * Pretty print the verification output of this report
     * @param out the output location
     */
    void print(PrintStream out);


    public static class Report {
        /**
         * Create a report of all difference reports in the collection.
         * @param reports the collection of difference reports
         * @param ps location of output
         * @return the mapping names of all scripts that have output file differences
         * and their number of differences
         */
        public static Map<String, Integer> reportCollection(Collection<VerifyReport> reports, PrintStream ps) {
            Map<String, Integer> failedTestMap = new HashMap<String, Integer>();
            for (VerifyReport report : reports) {
                if (ps != null) {
                    report.print(ps);
                }
                if (report.hasErrors()) {
                    failedTestMap.put(report.getScriptFileName(),report.getNumberOfErrors());
                }
            }
            if (ps != null) {
                ps.println(reports.size()+" tests run. "+failedTestMap.size()+" failed output differencing.");
            }
            return failedTestMap;
        }
    }

}
