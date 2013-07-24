package com.splicemachine.nist;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import difflib.Chunk;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.derby.tools.ij;
import org.junit.Assert;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class BaseNistTest {
    public static int DEFAULT_THREAD_POOL_SIZE = 1;

    public static final String DERBY_FILTER = "derby.filter";
    public static final String SPLICE_FILTER = "splice.filter";

    public static final String DERBY_OUTPUT_EXT = ".derby";
    public static final String SPLICE_OUTPUT_EXT = ".splice";

    // SQL files listed in this file are to be skipped from testing
    public static final String SKIP_TESTS_FILE_NAME = "skip.tests";

    // SQL files listed in this file contain "create schema..." directives
    // that are needed by other tests too.  Run these first.
    public static final String SCHEMA_LIST_FILE_NAME = "schema.list";

    public static List<String> SCHEMA_FILES = new ArrayList<String>();
	public static List<String> SKIP_TESTS = new ArrayList<String>();
    public static List<String> NON_TEST_FILES_TO_FILTER = new ArrayList<String>();

    public static List<File> getTestFileList() {
        // load SKIP_TESTS
        for (String baseName : fileToLines(getResourceDirectory() + "/nist/"+SKIP_TESTS_FILE_NAME, "#")) {
            SKIP_TESTS.add(baseName + ".sql");
        }
        // load SCHEMA_FILES
        for (String schemaFile : fileToLines(getResourceDirectory() + "/nist/"+SCHEMA_LIST_FILE_NAME, "#")) {
            SCHEMA_FILES.add(schemaFile);
        }
        // remove all SKIP_TESTS from SCHEMA_FILES
        SCHEMA_FILES.removeAll(SKIP_TESTS);

        // collect all non test files so that they can be filtered
        NON_TEST_FILES_TO_FILTER.addAll(BaseNistTest.SKIP_TESTS);
        NON_TEST_FILES_TO_FILTER.add(SKIP_TESTS_FILE_NAME);
        NON_TEST_FILES_TO_FILTER.add(SCHEMA_LIST_FILE_NAME);
        NON_TEST_FILES_TO_FILTER.add(DERBY_FILTER);
        NON_TEST_FILES_TO_FILTER.add(SPLICE_FILTER);
        // Adding schema files to be filtered here too. They will be re-added later to front
        // so that they run first.
        NON_TEST_FILES_TO_FILTER.addAll(BaseNistTest.SCHEMA_FILES);

        // this is the list of all test sql files, except schema creators
        List<File> testFiles2 = new ArrayList<File>(FileUtils.listFiles(new File(BaseNistTest.getResourceDirectory(), "/nist"),
                // exclude skipped and other non-test files
                new BaseNistTest.SpliceIOFileFilter(null, BaseNistTest.NON_TEST_FILES_TO_FILTER),
                null));

        // NIST sql files must be in sorted order
        Collections.sort(testFiles2, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                return file1.getName().compareTo(file2.getName());
            }
        });

        // Now create a new List with schema files in front
        List<File> testFiles = new ArrayList<File>(FileUtils.listFiles(new File(BaseNistTest.getResourceDirectory(), "/nist"),
                // include schema files
                new BaseNistTest.SpliceIOFileFilter(BaseNistTest.SCHEMA_FILES, null),
                null));

        // NIST sql files must be in sorted order
        Collections.sort(testFiles, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                return file1.getName().compareTo(file2.getName());
            }
        });

        // finally, add rest of test files to end of list after schema files
        testFiles.addAll(testFiles2);

        return testFiles;

    }

    public static List<String> readDerbyFilters() {
        List<String> derbyFilters = new ArrayList<String>();
        for (String filter :  fileToLines(getResourceDirectory() + "/nist/"+DERBY_FILTER, "#")) {
            derbyFilters.add(filter.trim());
        }
        return derbyFilters;
    }

    public static List<String> readSpliceFilters() {
        List<String> spliceFilters = new ArrayList<String>();
        for (String filter :  fileToLines(getResourceDirectory() + "/nist/"+SPLICE_FILTER, "#")) {
            spliceFilters.add(filter.trim());
        }
        return spliceFilters;
    }

    public static void runTest(File file, String type, Connection connection) throws Exception {
        FileInputStream fis = null;
        FileOutputStream fop = null;
        try {
            fis = new FileInputStream(file);
            File targetFile = new File(getBaseDirectory()+"/target/nist/" + file.getName().replace(".sql", type));
            Files.createParentDirs(targetFile);
            if (targetFile.exists())
                targetFile.delete();
            targetFile.createNewFile();
            fop = new FileOutputStream(targetFile);
            ij.runScript(connection, fis,"UTF-8",fop,"UTF-8");
            fop.flush();
        } catch (Exception e) {
            try {
                connection.close();
            } catch (SQLException e1) {
                // ignore
            }
            throw e;
        } finally {
            Closeables.closeQuietly(fop);
            Closeables.closeQuietly(fis);
        }
    }

    public static void assertNoDiffs(Collection<DiffReport> reports) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        int success = 0;
        for (BaseNistTest.DiffReport report : reports) {
            report.print(ps);
            if (! report.isEmpty()) {
                success++;
            }
        }

        Assert.assertEquals("Some test comparison failed: "+baos.toString("UTF-8")+"\n"+reports.size()+" Tests had differencs.", 0, success);
    }

    public static class SpliceIOFileFilter implements IOFileFilter {
		private List<String> inclusions;
		private List<String> exclusions;
		public SpliceIOFileFilter(List<String> inclusions, List<String> exclusions) {
			this.inclusions = inclusions;
			this.exclusions = exclusions;
		}

		@Override
		public boolean accept(File file) {
			if (inclusions != null) {
				if (inclusions.contains(file.getName()))
					return true;
				else 
					return false;
			}
			if (exclusions != null) {
				if (exclusions.contains(file.getName()))
					return false;
			}
			return true;
		}

		@Override
		public boolean accept(File dir, String name) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}

    public static List<String> fileToLines(String filename, String commentPattern) {
        List<String> lines = new LinkedList<String>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(filename));

            String line = in.readLine();
            while(line != null) {
                if (commentPattern != null) {
                    if (! line.startsWith(commentPattern)) {
                    lines.add(line);
                    }
                } else {
                    lines.add(line);
                }
                line = in.readLine();
            }
        } catch (IOException e) {
           Assert.fail("Unable to read: " + filename);
        }
        return lines;
    }

    public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_test"))
	    	userDir = userDir+"/structured_test";
	    return userDir;
	}

	public static String getResourceDirectory() {
		return getBaseDirectory()+"/src/test/resources";
	}

    public static List<DiffReport> diffOutput(List<File> sqlFiles,
                                              List<String> derbyFilter,
                                              List<String> spliceFilter) {

        List<DiffReport> diffs = new ArrayList<DiffReport>();

        for (File sqlFile: sqlFiles) {
            // derby output
            String derbyFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" +
                    sqlFile.getName().replace(".sql", BaseNistTest.DERBY_OUTPUT_EXT);
            List<String> derbyFileLines = fileToLines(derbyFileName, "--");
            // filter derby warnings, etc
            derbyFileLines = filterOutput(derbyFileLines, derbyFilter);

            // splice output
            String spliceFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" +
                    sqlFile.getName().replace(".sql", BaseNistTest.SPLICE_OUTPUT_EXT);
            List<String> spliceFileLines = fileToLines(spliceFileName, "--");
            // filter splice warnings, etc
            spliceFileLines = filterOutput(spliceFileLines, spliceFilter);

            Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

            DiffReport diff = new DiffReport(derbyFileName, spliceFileName);
            reportDeltas(patch.getDeltas(), diff);
            diffs.add(diff);
        }
        return diffs;
    }

    private static List<String> filterOutput(List<String> fileLines, List<String> warnings) {
        if (fileLines == null) {
            return null;
        }
        List<String> filteredLines = new ArrayList<String>(fileLines.size());
        for (String line : fileLines) {
            boolean filter = false;
            for (String warning : warnings) {
                if (line.contains(warning)) {
                    filter = true;
                }
            }
            if (! filter) {
                filteredLines.add(line);
            }
        }
        return filteredLines;
    }

    public static void reportDeltas(List<Delta> deltas, DiffReport diff) {
        for (Delta delta: deltas) {
            if (filterDeltas(delta)){
                continue;
            }
            diff.add("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
            printChunk(diff.derbyFile, delta.getOriginal(), diff);
            diff.add("++++++++++++++++++++++++++\n");
            printChunk(diff.spliceFile, delta.getRevised(), diff);
            diff.add(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
        }
    }

    public static void printChunk(String testType, Chunk chunk, DiffReport diff) {
        diff.add(testType+"\nPosition "+chunk.getPosition()+": \n");
        for(Object line : chunk.getLines()) {
            diff.add("  "+line+"\n");
        }
    }

    public static boolean filterDeltas(Delta delta) {
        Chunk chunk = delta.getOriginal();
        if (chunk.getLines() == null || chunk.getLines().isEmpty()){
            return true;
        }
        if (chunk.getLines().get(0).toString().startsWith("CONNECTION")) {
            return true;
        }
        // default
        return false;
    }

    public static class DiffReport {
        public final String derbyFile;
        public final String spliceFile;
        public List<String> report = new ArrayList<String>();

        public DiffReport(String derbyFile, String spliceFile) {
            this.derbyFile = derbyFile;
            this.spliceFile = spliceFile;
        }

        public void add(String report) {
            this.report.add(report);
        }

        public boolean isEmpty() {
            return report.isEmpty();
        }

        public void print(PrintStream out) {
            out.println("\n===========================================================================================");
            out.println(sqlName(derbyFile));
            if (report.isEmpty()) {
                out.println("  No difference");
            } else {
                for (String line : report) {
                    out.print(line);
                }
            }
            out.println("===========================================================================================");
        }
    }

    private static String sqlName(String fullName) {
        String[] cmpnts = StringUtils.split(fullName, '/');
        String baseName = cmpnts[cmpnts.length-1];
        baseName = baseName.replace(".derby",".sql");
        return baseName;
    }

    public static String getDuration(long startMilis, long stopMilis) {
        long secondInMillis = 1000;
        long minuteInMillis = secondInMillis * 60;
        long hourInMillis = minuteInMillis * 60;

        long diff = stopMilis - startMilis;
        long elapsedHours = diff / hourInMillis;
        diff = diff % hourInMillis;
        long elapsedMinutes = diff / minuteInMillis;
        diff = diff % minuteInMillis;
        long elapsedSeconds = diff / secondInMillis;
        diff = diff % secondInMillis;

        return String.format("%d hrs %02d min %02d sec %03d mil", elapsedHours, elapsedMinutes, elapsedSeconds, diff);
    }

}
