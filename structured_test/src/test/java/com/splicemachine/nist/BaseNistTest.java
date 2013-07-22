package com.splicemachine.nist;

import java.io.*;
import java.sql.Connection;
import java.util.*;

import difflib.Chunk;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.derby.tools.ij;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.junit.Assert;

public class BaseNistTest {
    public static final String DERBY_OUTPUT_EXT = ".derby";
    public static final String SPLICE_OUTPUT_EXT = ".splice";

    public static final String SKIP_TESTS_FILE_NAME = "skip.tests";
    public static final String SCHEMA_LIST_FILE_NAME = "schema.list";
    public static List<String> SCHEMA_FILES = new ArrayList<String>();
	public static List<String> SKIP_TESTS = new ArrayList<String>();
    public static List<String> NON_TEST_FILES_TO_FILTER = new ArrayList<String>();

    public static void loadFilteredFiles() {
        // load SKIP_TESTS
        for (String baseName :  fileToLines(getResourceDirectory() + "/nist/"+SKIP_TESTS_FILE_NAME, "#")) {
            SKIP_TESTS.add(baseName + ".sql");
        }
        // load SCHEMA_FILES
        for (String schemaFile :  fileToLines(getResourceDirectory() + "/nist/"+SCHEMA_LIST_FILE_NAME, "#")) {
            SCHEMA_FILES.add(schemaFile);
        }
        // remove schema files from SKIP_TESTS
        SKIP_TESTS.removeAll(SCHEMA_FILES);

        // collect all none test files so that they can be filtered
        NON_TEST_FILES_TO_FILTER.addAll(BaseNistTest.SKIP_TESTS);
        NON_TEST_FILES_TO_FILTER.addAll(BaseNistTest.SCHEMA_FILES);
        NON_TEST_FILES_TO_FILTER.add(SKIP_TESTS_FILE_NAME);
        NON_TEST_FILES_TO_FILTER.add(SCHEMA_LIST_FILE_NAME);

        // remove control files from SKIP_TESTS
        SCHEMA_FILES.remove(SKIP_TESTS_FILE_NAME);
        SCHEMA_FILES.remove(SCHEMA_LIST_FILE_NAME);
        SKIP_TESTS.remove(SKIP_TESTS_FILE_NAME);
        SKIP_TESTS.remove(SCHEMA_LIST_FILE_NAME);
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
			throw e;
		} finally {
			Closeables.closeQuietly(fop);
			Closeables.closeQuietly(fis);
		}
	}

    public static Collection<DiffReport> diffOutput(Collection<String> testsToIgnore) {

        Collection<DiffReport> diffs = new ArrayList<DiffReport>();

        Collection<File> sqlFiles = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"), new SpliceIOFileFilter(null, NON_TEST_FILES_TO_FILTER), null);
        for (File sqlFile: sqlFiles) {
            String derbyFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" + sqlFile.getName().replace(".sql", BaseNistTest.DERBY_OUTPUT_EXT);
            List<String> derbyFileLines = fileToLines(derbyFileName, null);

            String spliceFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" + sqlFile.getName().replace(".sql", BaseNistTest.SPLICE_OUTPUT_EXT);
            List<String> spliceFileLines = fileToLines(spliceFileName, null);

            Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

            DiffReport diff = new DiffReport(derbyFileName, spliceFileName);
            reportDeltas(patch.getDeltas(), diff, testsToIgnore);
            if (! diff.isEmpty()) {
                diffs.add(diff);
            }
        }
        return diffs;
    }

    public static void reportDeltas(List<Delta> deltas, DiffReport diff, Collection<String> testsToIgnoret) {
        for (Delta delta: deltas) {
            if (filterDeltas(delta, testsToIgnoret)){
                continue;
            }
            diff.add("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
            printChunk("Derby", delta.getOriginal(), diff);
            diff.add("++++++++++++++++++++++++++\n");
            printChunk("Splice", delta.getRevised(), diff);
            diff.add(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
        }
    }

    public static void printChunk(String testType, Chunk chunk, DiffReport diff) {
        diff.add(testType+" Position "+chunk.getPosition()+": \n");
        for(Object line : chunk.getLines()) {
            diff.add("  "+line+"\n");
        }
    }

    public static boolean filterDeltas(Delta delta, Collection<String> testsToIgnore) {
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
            out.println("Derby file: "+derbyFile+"  Splice File: "+spliceFile);
            for (String line : report) {
                out.print(line);
            }
            out.println("===========================================================================================");
        }
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
