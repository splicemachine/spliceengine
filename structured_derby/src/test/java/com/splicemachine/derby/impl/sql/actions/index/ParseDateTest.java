package com.splicemachine.derby.impl.sql.actions.index;

import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Jeff Cunningham
 *         Date: 8/27/13
 */
public class ParseDateTest {

    @Test
    public void testParseScriptOutput() throws Exception {
        String fileDirName = "/Users/jeff/dev/AWS/results/load/";
        List<String> csvs = createCSV(fileDirName, getFileNames(fileDirName, "load25_SI_NOPK\\d.log"));

        for (String csv : csvs) {
            System.out.println(csv);
        }
    }

    @Test
    public void testParseAveScriptOutput() throws Exception {
        String fileDirName = "/Users/jeff/dev/AWS/results/load/";
        List<String> csvs = createCSVMean(fileDirName, getFileNames(fileDirName, "load25_SI_NOPK\\d.log"));

        for (String csv : csvs) {
            System.out.println(csv);
        }
    }

    private List<String> getFileNames(String fileDirName, String filterPattern) {
        File fileDir = new File(fileDirName);
        final Pattern pattern = Pattern.compile(filterPattern);
        return Arrays.asList(fileDir.list(new FilenameFilter() {
            public boolean accept(File directory, String fileName) {
                return pattern.matcher(fileName).matches();
            }
        }));
    }

    private List<String> createCSV(String fileDir, List<String> fileNames) throws Exception {
        StringBuilder columnHeader = new StringBuilder();
        Map<String, List<DatePair>> filesToPairs = new TreeMap<String, List<DatePair>>();
        String[] phases = new String[] {"Load", "Create Index", "Query1"};
        for (String fileName : fileNames) {
            columnHeader.append(',').append(getName(fileName));
            filesToPairs.put(fileName, parseScript(fileDir + fileName, phases));
        }

        List<String> csvs = new ArrayList<String>();
        csvs.add(columnHeader.toString());
        csvs.add(csv(getSlice(fileNames, filesToPairs, 0), phases[0], false));
        csvs.add(csv(getSlice(fileNames, filesToPairs, 1), phases[1], false));
        csvs.add(csv(getSlice(fileNames, filesToPairs, 2), phases[2], false));
        return csvs;
    }

    private List<String> createCSVMean(String fileDir, List<String> fileNames) throws Exception {
        Map<String, List<DatePair>> filesToPairs = new TreeMap<String, List<DatePair>>();
        String[] phases = new String[] {"Load", "Create Index", "Query1"};
        for (String fileName : fileNames) {
            filesToPairs.put(fileName, parseScript(fileDir + fileName, phases));
        }

        List<String> csvs = new ArrayList<String>();
        csvs.add(getName(fileNames.get(0)));
        csvs.add(csvMean(getSlice(fileNames, filesToPairs, 0), phases[0]));
        csvs.add(csvMean(getSlice(fileNames, filesToPairs, 1), phases[1]));
        csvs.add(csvMean(getSlice(fileNames, filesToPairs, 2), phases[2]));
        return csvs;
    }

    private String csvMean(List<DatePair> testPairs, String rowHeader) throws Exception {
        long durationMean = calcDurationMean(testPairs);
        StringBuilder buf = new StringBuilder(rowHeader);
        buf.append(',').append(durationMean);
        return buf.toString();
    }

    private long calcDurationMean(List<DatePair> pairs) throws Exception {
        int nPairs = 0;
        long sum = 0;
        for (DatePair pair : pairs) {
            if (pair.phase != null) {
                sum += pair.getDuration();
                ++nPairs;
            }
        }
        return Math.round(sum / nPairs) / 1000;
    }

    private List<DatePair> getSlice(List<String> fileNames, Map<String, List<DatePair>> filesToPairs, int index) throws ParseException {
        List<DatePair> row = new ArrayList<DatePair>();
        for (String fileName : fileNames) {
            List<DatePair> value = filesToPairs.get(fileName);
            if (! value.isEmpty() && value.size() > index) {
                row.add(value.get(index));
            } else {
                row.add(new DatePair(null, null, null));
            }
        }
        return row;
    }

    private String csv(List<DatePair> testPairs, String rowHeader, boolean formatted) throws Exception {
        StringBuilder buf = new StringBuilder(rowHeader);
        for (DatePair pair : testPairs) {
            buf.append(',');
            long duration = pair.getDuration();
            if (duration > 0) {
                if (formatted) {
                    buf.append(formatDuration(duration));
                } else {
                    buf.append(duration/1000);
                }
            }
        }
        return buf.toString();
    }
    
    public static String formatDuration(long duration) {
        long secondInMillis = 1000;
        long minuteInMillis = secondInMillis * 60;
        long hourInMillis = minuteInMillis * 60;

        long diff = duration;
        long elapsedHours = diff / hourInMillis;
        diff = diff % hourInMillis;
        long elapsedMinutes = diff / minuteInMillis;
        diff = diff % minuteInMillis;
        long elapsedSeconds = diff / secondInMillis;
        diff = diff % secondInMillis;

        return String.format("%d hrs %02d min %02d sec %03d mil", elapsedHours, elapsedMinutes, elapsedSeconds, diff);
    }

    private class DatePair implements Comparable<DatePair> {
        private final String phase;
        private final long start;
        private final long end;

        public DatePair(String phase, String startStr, String endStr) throws ParseException {
            this.phase = phase;
            if (startStr != null && endStr != null) {
                this.start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(startStr).getTime();
                this.end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(endStr).getTime();
            } else {
                this.start = 0;
                this.end = 0;
            }
        }

        public long getDuration() {
            return end - start;
        }

        @Override
        public int compareTo(DatePair that) {
            long thisDuration = this.getDuration();
            long thatDuration = that.getDuration();
            return ((thisDuration > thatDuration) ? 1 : ((thisDuration < thatDuration) ? -1 : 0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DatePair datePair = (DatePair) o;

            return end == datePair.end && start == datePair.start;

        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }

    private List<DatePair> parseScript(String filePath, String[] phases) throws ParseException {
        List<String> lines = CsvUtil.fileToLines(filePath, null);
        List<DatePair> datePairs = new ArrayList<DatePair>();
        int phase = 0;
        String start = null;
        for (String line : lines) {
            if (line.startsWith("2013-")) {
                if (start != null) {
                    datePairs.add(new DatePair(phases[phase++], start, line.trim()));
                    start = null;
                } else {
                    start = line.trim();
                }
            }
        }
        return datePairs;
    }

    private String getName(String filePath) {
        String[] components = filePath.split("/");
        String name = components[components.length-1];
        int index = name.lastIndexOf(".");
        return (index >= 0 ? name.substring(0, index) : name);
    }

}
