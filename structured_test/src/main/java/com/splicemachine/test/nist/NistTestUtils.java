package com.splicemachine.test.nist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.splicemachine.test.utils.TestUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.derby.tools.ij;
import org.junit.Assert;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.diff.DiffReport;

/**
 * Static utility class to support various operations to run the Derby NIST SQL Scripts.
 */
public class NistTestUtils extends TestUtils {
    public static int DEFAULT_THREAD_POOL_SIZE = 4;

    public static final String HASH_COMMENT = "#";

    public static final String NIST_DIR = "/nist";
    public static final String NIST_DIR_SLASH = NIST_DIR+"/";
    public static final String TARGET_NIST_DIR = "/target"+NIST_DIR_SLASH;

    // These files contain warnings, errors to ignore
    public static final String DERBY_FILTER = "derby.filter";
    public static final String SPLICE_FILTER = "splice.filter";

    public static final String SQL_FILE_EXT = ".sql";
    public static final String DERBY_OUTPUT_EXT = ".derby";
    public static final String SPLICE_OUTPUT_EXT = ".splice";

    // SQL scripts listed in this file are to be skipped from testing
    public static final String SKIP_TESTS_FILE_NAME = "skip.tests";

    // SQL scripts listed in this file contain "create schema..." directives
    // that are needed by other tests too.  Run these first.
    public static final String SCHEMA_LIST_FILE_NAME = "schema.list";

    /**
     * Determine the order of, and the list of SQL scrips to run before
     * the given script so that it can run.<br/>
     *
     * @param scriptFileName the single script to run
     * @return the list of SQL script files to run in the order they should be run.
     */
    public static List<File> createRunList(String scriptFileName) {
        String sqlFileName = (scriptFileName.endsWith(".sql")? scriptFileName : scriptFileName+".sql");
        List<String> runListNames = getSchemaFileNames();
        if (! runListNames.contains(sqlFileName)) {
            runListNames.add(sqlFileName);
        } else {
            // if sqlFileName is one of the schema files,
            // as it is here, there's no need to run scripts that
            // are in list after this one - trim
            int index = runListNames.indexOf(sqlFileName);
            if (index < runListNames.size()) {
                for (int i=runListNames.size()-1; i>index; i--) {
                    runListNames.remove(i);
                }
            }
        }

        // turn them into files
        List<File> runList = createFiles(runListNames);
        return runList;
    }

    /**
     * Determine the order of, and the list of SQL scrips to run using information
     * from various files in the SQL script directory.<br/>
     *
     * @return the list of SQL script files to run in the order they should be run.
     */
    public static List<File> getTestFileList() {
        List<String> schemaFileNames = getSchemaFileNames();

        // create a list with schema files in front
//        List<File> testFiles = new ArrayList<File>(FileUtils.listFiles(new File(getResourceDirectory(), NIST_DIR),
//                // include schema files
//                new SpliceIOFileFilter(schemaFileNames, null), null));
        List<File> testFiles = createFiles(schemaFileNames);

        // collect all non test files so that they can be filtered
        List<String> fileNamesToFilter = getExcludedFileNames();
        // Adding schema files to be filtered here too. They will be in front of
        // testFiles list so that they run first.
        fileNamesToFilter.addAll(schemaFileNames);

        // this is the list of all test sql files, except schema creators
        // and non test files
        List<File> testFiles2 = new ArrayList<File>(FileUtils.listFiles(new File(getResourceDirectory(), NIST_DIR),
                // exclude skipped and other non-test files
                new SpliceIOFileFilter(null, fileNamesToFilter), null));

        // NIST sql files must be in sorted order
        Collections.sort(testFiles2, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                return file1.getName().compareTo(file2.getName());
            }
       });

        // finally, append rest of test files to end of list after schema files
        testFiles.addAll(testFiles2);

        return testFiles;
    }

    /**
     * Create files from nist test script names. Preserves order.
     * @param fileNames names of files to create.
     * @return a list of files in given file name order.
     */
    public static List<File> createFiles(List<String> fileNames) {
        List<File> runList = new ArrayList<File>(fileNames.size());
        for (String fileName : fileNames) {
            runList.add(new File(getResourceDirectory()+NIST_DIR_SLASH+fileName));
        }
        return runList;
    }

    /**
     * Get the list of nist file name scripts that create schema
     * @return the schema creators
     */
    public static List<String> getSchemaFileNames() {
        List<String> schemaFileNames = new ArrayList<String>();
        // load all schema creation scripts
        for (String schemaFile : fileToLines(getResourceDirectory() + NIST_DIR_SLASH +SCHEMA_LIST_FILE_NAME, HASH_COMMENT)) {
            schemaFileNames.add(schemaFile);
        }
        // remove all skipped test scripts from schema creators
        schemaFileNames.removeAll(getSkipTestFileNames());
        return schemaFileNames;
    }

    /**
     * Get the list of nist script files to skip
     * @return the test scripts to skip
     */
    public static List<String> getSkipTestFileNames() {
        List<String> skipTestFileNames = new ArrayList<String>();
        // load skip test file names from file
        for (String baseName : fileToLines(getResourceDirectory() + NIST_DIR_SLASH +SKIP_TESTS_FILE_NAME, HASH_COMMENT)) {
            skipTestFileNames.add(baseName + SQL_FILE_EXT);
        }
        return skipTestFileNames;
    }

    /**
     * Get the list of file names to exclude from testing.<br/>
     * Includes skip test file names and configuration files names
     * @return the list of all excluded file names
     */
    public static List<String> getExcludedFileNames() {
        List<String> nonTestFileNames = new ArrayList<String>();
        nonTestFileNames.addAll(getSkipTestFileNames());
        nonTestFileNames.add(SKIP_TESTS_FILE_NAME);
        nonTestFileNames.add(SCHEMA_LIST_FILE_NAME);
        nonTestFileNames.add(DERBY_FILTER);
        nonTestFileNames.add(SPLICE_FILTER);
        return nonTestFileNames;
    }

    /**
     * Read the list of Derby warning, error strings to filter (ignore) in the test output
     * in order to do a clean diff between derby and splice output.
     *
     * @return the list of warning, error strings to ignore
     */
    public static List<String> readDerbyFilters() {
        return readFilters(fileToLines(getResourceDirectory() + NIST_DIR_SLASH +DERBY_FILTER, HASH_COMMENT));
    }

    /**
     * Read the list of Splice warning, error strings to filter (ignore) in the test output
     * in order to do a clean diff between derby and splice output.
     *
     * @return the list of warning, error strings to ignore
     */
    public static List<String> readSpliceFilters() {
        return readFilters(fileToLines(getResourceDirectory() + NIST_DIR_SLASH +SPLICE_FILTER, HASH_COMMENT));
    }

    private static List<String> readFilters(List<String> fileLines) {
        List<String> filters = new ArrayList<String>(fileLines.size());
        for (String line :  fileLines) {
            String filter = line.trim();
            if (! filter.isEmpty()) {
                filters.add(filter);
            }
        }
        return filters;
    }

    /**
     * Execute a set of NIST SQL scripts, capture, filter and difference the output.
     * @param testFiles the suite of SQL scripts to run
     * @param derbyRunner the Derby test runner
     * @param derbyOutputFilter the list of filters to apply to Derby output
     * @param spliceRunner the Splice test runner
     * @param spliceOutputFilter the list of filters to apply to Splice output
     * @param out the location to which to print the output.
     * @return the collection of difference reports.
     * @throws Exception any failure
     */
    public static Collection<DiffReport> runTests(List<File> testFiles,
                                                  DerbyNistRunner derbyRunner,
                                                  List<String> derbyOutputFilter,
                                                  SpliceNistRunner spliceRunner,
                                                  List<String> spliceOutputFilter,
                                                  PrintStream out) throws Exception {
        out.println("Starting...");
        // print to stdout also for user feedback...
        System.out.println("Starting...");

        // run derby
        out.println("Derby...");
        System.out.println("Derby...");

        out.println("    Running "+testFiles.size()+" tests...");
        System.out.println("    Running "+testFiles.size()+" tests...");
        long start = System.currentTimeMillis();
        derbyRunner.runDerby(testFiles);
        String derbyDone = "    Duration: " + getDuration(start, System.currentTimeMillis());
        out.println(derbyDone);
        System.out.println(derbyDone);

        // run splice
        out.println("Splice...");
        System.out.println("Splice...");

        out.println("    Running "+testFiles.size()+" tests...");
        System.out.println("    Running "+testFiles.size()+" tests...");
        start = System.currentTimeMillis();
        spliceRunner.runSplice(testFiles);
        String spliceDone = "    Duration: " + getDuration(start, System.currentTimeMillis());
        out.println(spliceDone);
        System.out.println(spliceDone);

        // diff output and assert no differences in each report
        Collection<DiffReport> reports = DiffEngine.diffOutput(testFiles,
                getBaseDirectory() + TARGET_NIST_DIR, derbyOutputFilter, spliceOutputFilter);

        return reports;
    }

    /**
     * The workhorse of test execution.
     * <p>
     *     This method, called by the test runners, executes a single script against a
     *     database connection using Derby's ij execution framework.
     * </p>
     * @param file the SQL script file to run
     * @param outputFileExtension the output file extension indicating the type of test runner executing
     *             the test - one of {@link #DERBY_OUTPUT_EXT} or {@link #SPLICE_OUTPUT_EXT}.<br/>
     *                            ij will write test output to a file with the SQL script base name and
     *                            this extension.
     * @param connection the connection to execute the script against. This will ether be an
     *                   embedded derby connection or a networked splice connection.
     * @throws Exception thrown upon any error condition. The file input, output as well as the connection
     * is closed.
     */
    public static void runTest(File file, String outputFileExtension, Connection connection) throws Exception {
        FileInputStream fis = null;
        FileOutputStream fop = null;
        try {
            fis = new FileInputStream(file);
            File targetFile = new File(getBaseDirectory()+ TARGET_NIST_DIR + file.getName().replace(SQL_FILE_EXT, outputFileExtension));
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

    /**
     * Write the given <code>content</code> to the given <code>logName</code>
     * to be created in the given <code>dirName</code>.
     * @param dirName full path to the directory in which to create the log.
     * @param logName the name of the log file.
     * @param discriminator a file discriminator to append to <code>logName</code>
     *                      when writing multiple copies of the same file. Ignored
     *                      if <code>null</code>.
     * @param content the content of which to write to the file.
     * @throws Exception any failure.
     */
    public static void createLog(String dirName, String logName, String discriminator, String content) throws Exception {
        File targetFile = new File(dirName,logName+(discriminator != null ? discriminator : ""));
        Files.createParentDirs(targetFile);
        if (targetFile.exists())
            targetFile.delete();
        targetFile.createNewFile();
        FileUtils.writeStringToFile(targetFile, content);
    }

    /**
     * File filter to use when determining types of files to be included in a list.
     */
    public static class SpliceIOFileFilter implements IOFileFilter {
		private final List<String> inclusions;
		private final List<String> exclusions;
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
			// only accepting files
			return false;
		}
		
	}

    /**
     * Read files into a list of Strings optionally ignoring comment lines.
     *
     * @param filePath the full path of the file to read.
     * @param commentPattern the optional beginning line comment to
     *                       ignore.
     * @return the list of lines from the file with any comment lines optionally
     * absent.
     * @see #lineIsComment(String, String)
     */
    public static List<String> fileToLines(String filePath, String commentPattern) {
        List<String> lines = new LinkedList<String>();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filePath));

            String line = in.readLine();
            while(line != null) {
                if (commentPattern != null) {
                    if (! lineIsComment(line, commentPattern)) {
                    lines.add(line);
                    }
                } else {
                    lines.add(line);
                }
                line = in.readLine();
            }
        } catch (IOException e) {
           Assert.fail("Unable to read: " + filePath+": "+e.getLocalizedMessage());
        } finally {
        	if (in != null) {
        		try {
					in.close();
				} catch (IOException e) {
					// ignore
				}
        	}
        }
        return lines;
    }

    /**
     * Returns <code>true</code> if a the given string starts with the
     * <code>commentPattern</code>
     *
     * @param line the string to consider with leading whitespace ignored
     * @param commentPattern the comment pattern to employ
     * @return <code>true</code> if and only if the line begins with
     * the <code>commentPattern</code>
     */
    public static boolean lineIsComment(String line, String commentPattern) {
        if (commentPattern == null || commentPattern.isEmpty()) {
            return false;
        }
        return line.trim().startsWith(commentPattern);
    }

    /**
     * Get the base directory of this (structure_test) project
     * @return the full path of the project base directory
     */
    public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_test"))
	    	userDir = userDir+"/structured_test";
	    return userDir;
	}

    /**
     * Get the resource directory (&lt;projectBaseDirectory&gt;/src/test/resources)
     * for this project (structured_test)
     * @return the full path of the project resource directory
     */
    public static String getResourceDirectory() {
		return getBaseDirectory()+"/src/test/resources";
	}

    /**
     * Calculate and return the string duration of the given start and end times (in milliseconds)
     * @param startMilis the starting time of the duration given by <code>System.currentTimeMillis()</code>
     * @param stopMilis the ending time of the duration given by <code>System.currentTimeMillis()</code>
     * @return example <code>0 hrs 04 min 41 sec 337 mil</code>
     */
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
