package com.splicemachine.test.nist.test;

import com.google.common.collect.Lists;
import com.splicemachine.test.nist.NistTestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static com.splicemachine.test.nist.NistTestUtils.*;

/**
 *
 * Tests only for testing framework.
 *
 * @author Jeff Cunningham
 *         Date: 7/19/13
 */
public class TestTheTestClassesTest {

    private static String printList(Collection<? extends Object> things) {
        StringBuilder buf = new StringBuilder("\n");
        for (Object thing : things) {
            buf.append(thing);
            buf.append('\n');
        }
        return buf.toString();
    }

    private void assertNoDuplicates(List<File> files) {
        Set<File> unique = new HashSet<File>(files);
        Assert.assertEquals("Contains duplicates",unique.size(),files.size());
    }

    @Test
    public void testReadFile() throws Exception {
        List<String> lines = fileToLines(getResourceDirectory() + NIST_DIR_SLASH+"skip.tests", "#");
        Assert.assertFalse("Got nuthin", lines.isEmpty());
        for (String line : lines) {
            Assert.assertFalse("Unexpected comment string: #", line.startsWith("#"));
        }

        lines = fileToLines(getResourceDirectory() + NIST_DIR_SLASH+"cdr002.sql", "--");
        Assert.assertFalse("Got nuthin", lines.isEmpty());
        for (String line : lines) {
            Assert.assertFalse("Unexpected comment string: --", line.startsWith("--"));
        }
    }

    @Test
    public void testFileFilters() throws Exception {
        List<String> skipTestFileNames = getSkipTestFileNames();
        List<String> schemaFileNames = getSchemaFileNames();
        List<String> nonTestFileNames = getExcludedFileNames();

        // test files to skip filter
        List<String> filter = Lists.newArrayList(skipTestFileNames);
        filter.addAll(schemaFileNames);
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(), NIST_DIR),
                new NistTestUtils.SpliceIOFileFilter(null, filter), null);

        Assert.assertTrue(files.contains(new File(getResourceDirectory(), NIST_DIR_SLASH+"cdr002.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), skipTestFileNames.contains(file.getName()));
            Assert.assertFalse(printList(files), schemaFileNames.contains(file.getName()));
        }
        Assert.assertTrue(nonTestFileNames.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(nonTestFileNames.contains(SKIP_TESTS_FILE_NAME));

        // test schema files filter
        filter = Lists.newArrayList(schemaFileNames);
        filter.addAll(skipTestFileNames);
        files = FileUtils.listFiles(new File(getResourceDirectory(), NIST_DIR),
                new NistTestUtils.SpliceIOFileFilter(null, filter), null);

        Assert.assertFalse(files.contains(new File(getResourceDirectory(), NIST_DIR_SLASH+"schema5.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), skipTestFileNames.contains(file.getName()));
            Assert.assertFalse(printList(files), schemaFileNames.contains(file.getName()));
        }
        Assert.assertTrue(nonTestFileNames.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(nonTestFileNames.contains(SKIP_TESTS_FILE_NAME));
        Assert.assertTrue(nonTestFileNames.contains(DERBY_FILTER));
        Assert.assertTrue(nonTestFileNames.contains(SPLICE_FILTER));
    }

    @Test
    public void testGetSchemaFilesExist() throws Exception {
        List<String> schemaFileNames = getSchemaFileNames();
        List<File> schemaFiles = createFiles(schemaFileNames);
        assertNoDuplicates(schemaFiles);
        Assert.assertEquals(schemaFileNames.size(),schemaFiles.size());
        for (File aFile : schemaFiles) {
            Assert.assertTrue(aFile + " does not exist",aFile.exists());
        }
        Assert.assertTrue("schema1.sql should be first",
                schemaFiles.get(0).equals(new File(getResourceDirectory() + NIST_DIR_SLASH+"schema1.sql")));
    }

    @Test
    public void testGetFilesExclude() throws Exception {
        List<String> schemaFileNames = getSchemaFileNames();
        List<String> excludedFileNames = getExcludedFileNames();
        excludedFileNames.addAll(schemaFileNames);

        // this is the list of all test sql files, except schema creators
        // and non test files
        List<File> testFiles = new ArrayList<File>(FileUtils.listFiles(new File(getResourceDirectory(), NIST_DIR),
                // exclude skipped and other non-test files
                new SpliceIOFileFilter(null, excludedFileNames), null));
        assertNoDuplicates(testFiles);

        List<String> testFileNames = new ArrayList<String>();
        for (File aFile : testFiles) {
            Assert.assertFalse(schemaFileNames.contains(aFile.getName()));
            testFileNames.add(aFile.getName());
        }

        Collections.sort(testFileNames, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        schemaFileNames.addAll(testFileNames);
    }

    @Test
    public void testGetTestFilesExist() throws Exception {
        List<File> testFiles = getTestFileList();
        for (File aFile : testFiles) {
            Assert.assertTrue(aFile.getCanonicalPath()+" does not exist",aFile.exists());
        }
    }

    @Test
    public void testGetTestFilesNoDuplicates() throws Exception {
        List<File> testFiles = getTestFileList();
        Assert.assertFalse("Got nuthin", testFiles.isEmpty());
        assertNoDuplicates(testFiles);
    }

    @Test
    public void testGetTestFilesOrder() throws Exception {
        List<File> testFiles = getTestFileList();
        File schema1 = new File(getResourceDirectory() + NIST_DIR_SLASH+"schema1.sql");
        Assert.assertTrue("Missing schema1.sql", testFiles.contains(schema1));
        Assert.assertTrue("schema1.sql should be first", testFiles.get(0).equals(schema1));
    }

    @Test
    public void testGetTestFilesAssertSorted() throws Exception {
        List<File> testFiles = getTestFileList();
        List<File> schemaFiles = new ArrayList<File>();
        for (String schemaFileName : getSchemaFileNames()) {
            schemaFiles.add(new File(getResourceDirectory() + NIST_DIR_SLASH+schemaFileName));
        }
        testFiles.removeAll(schemaFiles);
        List<File> sortedTestFiles = new ArrayList<File>(testFiles);

        Collections.sort(sortedTestFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        Assert.assertEquals(sortedTestFiles,testFiles);
    }

    @Test
    public void testCreateRunList() throws Exception {
        List<File> testFiles = createRunList("dml016.sql");
        Assert.assertFalse("Got nuthin", testFiles.isEmpty());
        for (File aFile : testFiles) {
            Assert.assertTrue(aFile.getCanonicalPath()+" does not exist",aFile.exists());
        }
        assertNoDuplicates(testFiles);
    }

    @Test
    public void testCreateRunListTrim() throws Exception {
        List<File> testFiles = createRunList("schema4.sql");
        Assert.assertEquals(7, testFiles.size());
        Assert.assertEquals("schema4.sql", testFiles.get(testFiles.size()-1).getName());
        for (File aFile : testFiles) {
            Assert.assertTrue(aFile.getCanonicalPath()+" does not exist",aFile.exists());
        }
        assertNoDuplicates(testFiles);
    }
}
