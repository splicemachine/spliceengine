package com.splicemachine.test.nist.test;

import com.google.common.collect.Lists;
import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.diff.DiffReport;
import com.splicemachine.test.nist.NistTestUtils;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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

    @Test
    public void testReadFile() throws Exception {
        List<String> lines = fileToLines(getResourceDirectory() + "/nist/skip.tests", "#");
        Assert.assertFalse("Got nuthin", lines.isEmpty());
        for (String line : lines) {
            Assert.assertFalse("Unexpected comment string: #", line.startsWith("#"));
        }

        lines = fileToLines(getResourceDirectory() + "/nist/cdr002.sql", "--");
        Assert.assertFalse("Got nuthin", lines.isEmpty());
        for (String line : lines) {
            Assert.assertFalse("Unexpected comment string: --", line.startsWith("--"));
        }
    }

    @Test
    public void testFileFilters() throws Exception {
        NistTestUtils.getTestFileList();
        Assert.assertFalse(SKIP_TESTS.isEmpty());
        Assert.assertFalse(SCHEMA_FILES.isEmpty());

        // test files to skip filter
        List<String> filter = Lists.newArrayList(NistTestUtils.SKIP_TESTS);
        filter.addAll(NistTestUtils.SCHEMA_FILES);
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"),
                new NistTestUtils.SpliceIOFileFilter(null, filter), null);

        Assert.assertTrue(files.contains(new File(getResourceDirectory(), "/nist/cdr002.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), SKIP_TESTS.contains(file.getName()));
            Assert.assertFalse(printList(files), SCHEMA_FILES.contains(file.getName()));
        }
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SKIP_TESTS_FILE_NAME));

        // test schema files filter
        filter = Lists.newArrayList(NistTestUtils.SCHEMA_FILES);
        filter.addAll(NistTestUtils.SKIP_TESTS);
        files = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"),
                new NistTestUtils.SpliceIOFileFilter(null, filter), null);

        Assert.assertFalse(files.contains(new File(getResourceDirectory(), "/nist/schema5.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), SKIP_TESTS.contains(file.getName()));
            Assert.assertFalse(printList(files), SCHEMA_FILES.contains(file.getName()));
        }
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SKIP_TESTS_FILE_NAME));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(DERBY_FILTER));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SPLICE_FILTER));
    }

    @Test
    public void testGetTestFiles() throws Exception {
        List<File> testFiles = NistTestUtils.getTestFileList();
        Assert.assertFalse("Got nuthin", testFiles.isEmpty());
        System.out.println(printList(testFiles));
        System.out.println(testFiles.size()+" files");
    }
}
