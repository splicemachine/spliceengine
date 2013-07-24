package com.splicemachine.nist.test;

import com.google.common.collect.Lists;
import com.splicemachine.nist.BaseNistTest;
import com.splicemachine.nist.DerbyNistTest;
import com.splicemachine.nist.SpliceNistTest;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.nist.BaseNistTest.*;

/**
 *
 * Tests only for testing framework.
 *
 * @author Jeff Cunningham
 *         Date: 7/19/13
 */
public class TestTheTestClassesTest {

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
        BaseNistTest.getTestFileList();
        Assert.assertFalse(SKIP_TESTS.isEmpty());
        Assert.assertFalse(SCHEMA_FILES.isEmpty());

        // test files to skip filter
        List<String> filter = Lists.newArrayList(BaseNistTest.SKIP_TESTS);
        filter.addAll(BaseNistTest.SCHEMA_FILES);
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"),
                new BaseNistTest.SpliceIOFileFilter(null, filter), null);

        Assert.assertTrue(files.contains(new File(getResourceDirectory(), "/nist/cdr002.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), SKIP_TESTS.contains(file.getName()));
            Assert.assertFalse(printList(files), SCHEMA_FILES.contains(file.getName()));
        }
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SKIP_TESTS_FILE_NAME));

        // test schema files filter
        filter = Lists.newArrayList(BaseNistTest.SCHEMA_FILES);
        filter.addAll(BaseNistTest.SKIP_TESTS);
        files = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"),
                new BaseNistTest.SpliceIOFileFilter(null, filter), null);

        Assert.assertFalse(files.contains(new File(getResourceDirectory(), "/nist/schema5.sql")));

        for (File file : files) {
            Assert.assertFalse(printList(files), SKIP_TESTS.contains(file.getName()));
            Assert.assertFalse(printList(files), SCHEMA_FILES.contains(file.getName()));
        }
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SCHEMA_LIST_FILE_NAME));
        Assert.assertTrue(NON_TEST_FILES_TO_FILTER.contains(SKIP_TESTS_FILE_NAME));
    }

    @Test
    public void testDiff() throws Exception {
        String derbyFileName = BaseNistTest.getResourceDirectory() + "/difftest/diff_cdr002.derby";
        List<String> derbyFileLines = fileToLines(derbyFileName, null);

        String spliceFileName = BaseNistTest.getResourceDirectory() + "/difftest/diff_cdr002.splice";
        List<String> spliceFileLines = fileToLines(spliceFileName, null);

        Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        BaseNistTest.DiffReport report = new BaseNistTest.DiffReport(derbyFileName, spliceFileName);
        BaseNistTest.reportDeltas(patch.getDeltas(), report);
        PrintStream out = System.out;
        report.print(out);
    }

    @Test
    public void testGetTestFiles() throws Exception {
        List<File> testFiles = BaseNistTest.getTestFileList();
        Assert.assertFalse("Got nuthin", testFiles.isEmpty());
        System.out.println(printList(testFiles));
        System.out.println(testFiles.size()+" files");
    }

    private String printList(Collection<? extends Object> things) {
        StringBuilder buf = new StringBuilder("\n");
        for (Object thing : things) {
            buf.append(thing);
            buf.append('\n');
        }
        return buf.toString();
    }
}
