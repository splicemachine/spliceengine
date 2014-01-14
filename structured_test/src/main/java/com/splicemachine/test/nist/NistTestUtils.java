package com.splicemachine.test.nist;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.splicemachine.test.utils.TestUtils;

/**
 * Static utility class to support various operations to run the Derby NIST SQL Scripts.
 */
public class NistTestUtils extends TestUtils {
    public static final String HASH_COMMENT = "#";

    public static final String NIST_DIR = "/nist";
    public static final String NIST_DIR_SLASH = NIST_DIR+"/";
    public static final String TARGET_NIST_DIR = "/target"+NIST_DIR_SLASH;

    // These files contain warnings, errors to ignore for Nist tests
    public static final String DERBY_FILTER = "derby.filter";
    public static final String SPLICE_FILTER = "splice.filter";

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
        List<File> testFiles2 = new ArrayList<File>(FileUtils.listFiles(new File(TestUtils.getResourceDirectory(), NIST_DIR),
                // exclude skipped and other non-test files
                new TestUtils.SpliceIOFileFilter(null, fileNamesToFilter), null));

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
            runList.add(new File(TestUtils.getResourceDirectory()+NIST_DIR_SLASH+fileName));
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
        for (String schemaFile : TestUtils.fileToLines(TestUtils.getResourceDirectory() + NIST_DIR_SLASH +SCHEMA_LIST_FILE_NAME, HASH_COMMENT)) {
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
        for (String baseName : TestUtils.fileToLines(TestUtils.getResourceDirectory() + NIST_DIR_SLASH +SKIP_TESTS_FILE_NAME, HASH_COMMENT)) {
            skipTestFileNames.add(baseName + TestUtils.SQL_FILE_EXT);
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
        return readFilters(TestUtils.fileToLines(TestUtils.getResourceDirectory() + NIST_DIR_SLASH +DERBY_FILTER, HASH_COMMENT));
    }

    /**
     * Read the list of Splice warning, error strings to filter (ignore) in the test output
     * in order to do a clean diff between derby and splice output.
     *
     * @return the list of warning, error strings to ignore
     */
    public static List<String> readSpliceFilters() {
        return readFilters(TestUtils.fileToLines(TestUtils.getResourceDirectory() + NIST_DIR_SLASH +SPLICE_FILTER, HASH_COMMENT));
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

}
