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

	private static final String FILEDIRNAME = "/Users/jeff/dev/AWS/results/load/";
	private static final List<String> PHASES = Arrays.asList("Load", "Create Index", "Query1"); 
	
    @Test
    public void testParseScriptOutput() throws Exception {
        List<String> csvs = createCSV(FILEDIRNAME, 
        		getFileNames(FILEDIRNAME, "load25.*\\.log"), PHASES);

        for (String csv : csvs) {
            System.out.println(csv);
        }
    }

    @Test
    public void testParseAveScriptOutput() throws Exception {
        List<String> csvs = createCSVMean(FILEDIRNAME, 
        		getFileNames(FILEDIRNAME, "load25_default_hbase_NOSI_PK\\d\\.log"), PHASES);

        for (String csv : csvs) {
            System.out.println(csv);
        }
    }

    public static List<String> createCSV(String fileDir, List<String> fileNames, List<String> phases) throws Exception {
        StringBuilder columnHeader = new StringBuilder();
        Map<String, List<DatePair>> filesToPairs = new TreeMap<String, List<DatePair>>();
        for (String fileName : fileNames) {
            columnHeader.append(',').append(getName(fileName));
            filesToPairs.put(fileName, parseScriptForDates(fileDir + fileName, phases));
        }

        List<String> csvs = new ArrayList<String>();
        csvs.add(columnHeader.toString());
        for (int i = 0; i < phases.size(); i++) {
			csvs.add(csv(getSlice(fileNames, filesToPairs, i), phases.get(i), false));
		}
		return csvs;
    }

    public static String csv(List<DatePair> testPairs, String rowHeader, boolean formatted) throws Exception {
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

    public static List<String> createCSVMean(String fileDir, List<String> filePaths, List<String> phases) throws Exception {
        List<String> csvs = new ArrayList<String>();
        Map<String, List<DatePair>> filesToPairs = new TreeMap<String, List<DatePair>>();
        for (String filePath : filePaths) {
            filesToPairs.put(filePath, parseScriptForDates(fileDir + filePath, phases));
			csvs.add(getName(filePath));
        }

		for (int i = 0; i < phases.size(); i++) {
			csvs.add(csvMean(getSlice(filePaths, filesToPairs, i), phases.get(i)));
		}
		return csvs;
    }

    public static String csvMean(List<DatePair> testPairs, String rowHeader) throws Exception {
        long durationMean = calcDurationMeanInSeconds(testPairs);
        StringBuilder buf = new StringBuilder(rowHeader);
        buf.append(',').append(durationMean);
        return buf.toString();
    }

    public static long calcDurationMeanInSeconds(List<DatePair> pairs) throws Exception {
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

    public static List<DatePair> getSlice(List<String> fileNames, Map<String, List<DatePair>> filesToPairs, int index) {
        List<DatePair> row = new ArrayList<DatePair>();
        for (String fileName : fileNames) {
            List<DatePair> value = filesToPairs.get(fileName);
            if (! value.isEmpty() && value.size() > index) {
                row.add(value.get(index));
            } else {
					row.add(DatePair.emptyDatePair());
            }
        }
        return row;
    }

    public static List<DatePair> parseScriptForDates(String filePath, List<String> phases) throws ParseException {
        List<String> lines = CsvUtil.fileToLines(filePath, null);
        List<DatePair> datePairs = new ArrayList<DatePair>();
        int phase = 0;
        String start = null;
        for (String line : lines) {
            if (line.startsWith("2013-")) {
                if (start != null) {
                    datePairs.add(new DatePair(phases.get(phase++), start, line.trim()));
                    start = null;
                } else {
                    start = line.trim();
                }
            }
        }
        return datePairs;
    }

    public static List<String> getFileNames(String fileDirName, String filterPattern) {
        File fileDir = new File(fileDirName);
        final Pattern pattern = Pattern.compile(filterPattern);
        return Arrays.asList(fileDir.list(new FilenameFilter() {
            public boolean accept(File directory, String fileName) {
                return pattern.matcher(fileName).matches();
            }
        }));
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

    private static String getName(String filePath) {
        String[] components = filePath.split("/");
        String name = components[components.length-1];
        int index = name.lastIndexOf(".");
        return (index >= 0 ? name.substring(0, index) : name);
    }

    public static class DatePair implements Comparable<DatePair> {
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

        public static DatePair emptyDatePair() {
        	DatePair empty = null;
			try {
				empty = new DatePair(null,null,null);
			} catch (ParseException e) {
				// won't happen
			}
			return empty;
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

}
