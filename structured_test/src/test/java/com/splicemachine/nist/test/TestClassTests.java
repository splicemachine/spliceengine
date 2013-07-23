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
public class TestClassTests {

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

//    @Test         Gotta find somewhere to keep around a couple of files to diff
    public void testDiff() throws Exception {
        File sqlFile = new File(BaseNistTest.getBaseDirectory() + "/target/nist/dml001.sql");
        String derbyFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" + sqlFile.getName().replace(".sql", BaseNistTest.DERBY_OUTPUT_EXT);
        List<String> derbyFileLines = fileToLines(derbyFileName, null);

        String spliceFileName = BaseNistTest.getBaseDirectory() + "/target/nist/" + sqlFile.getName().replace(".sql", BaseNistTest.SPLICE_OUTPUT_EXT);
        List<String> spliceFileLines = fileToLines(spliceFileName, null);

        Patch patch = DiffUtils.diff(derbyFileLines, spliceFileLines);

        BaseNistTest.DiffReport report = new BaseNistTest.DiffReport(derbyFileName, spliceFileName);
        BaseNistTest.reportDeltas(patch.getDeltas(), report, null);
        PrintStream out = System.out;
        report.print(out);
    }

    @Test
    public void testDerbySetup() throws Exception {
        DerbyNistTest.setup();
    }

    @Test
    public void testSpliceSetup() throws Exception {
        SpliceNistTest.setup();
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
