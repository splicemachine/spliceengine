package com.splicemachine.nist.test;

import com.google.common.collect.Lists;
import com.splicemachine.nist.BaseNistTest;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.nist.BaseNistTest.SKIP_TESTS;
import static com.splicemachine.nist.BaseNistTest.fileToLines;
import static com.splicemachine.nist.BaseNistTest.getResourceDirectory;

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
    public void testSkipTestsFilter() throws Exception {
        BaseNistTest.loadSkipTests(getResourceDirectory() + "/nist/skip.tests", "#");
        Assert.assertFalse(SKIP_TESTS.isEmpty());

        List<String> filter = Lists.newArrayList(BaseNistTest.SKIP_TESTS);
        filter.addAll(BaseNistTest.SCHEMA_SCRIPTS);
        Collection<File> files = FileUtils.listFiles(new File(getResourceDirectory(), "/nist"),
                new BaseNistTest.SpliceIOFileFilter(null, filter), null);

        Assert.assertTrue(files.contains(new File(getResourceDirectory(), "/nist/cdr002.sql")));
        for (File file : files) {
            Assert.assertFalse(SKIP_TESTS.contains(file.getName()));
            Assert.assertFalse(file.getName().equals("schema1.sql"));
        }
    }

    private void printList( Collection<? extends Object> things) {
        for (Object thing : things) {
            System.out.println(thing);
        }
    }

    private static List<String> EXPECTED_SKIP_TESTS = Arrays.asList(
//	#cdr007 - bug 555
//	#Added order by these failing tests due to mismatch.
//	# basetabs,dml001,dml012,dml015,dml 016,dml044
//	# dml046,dml058,dml060,dml068,xts730,yts812
//	# Bug 552, 553, 599
            "dml001",
//	# dml014 - bug 625
            "dml014",
//	# dml020: Bug 597 self joins
            "dml020",
//	# dml022: bug 492
            "dml022",
//	# dml023: This tests will always fail due to test 0107 where varchar column is compared with blank padded value.
//	#dml023
//	# dml024: bug 495
//	#dml024
//	#dml026 - The only difference is when there is  ERROR message output from query, Derby's output is different from Splice
            "dml026",
//	# dml034: bug384
            "dml034",
//	#dml035 - Bug 560
            "dml035",
//	# dml049: select from 10 tables in where clause
            "dml049",
//	# dml050: bug 492
            "dml050",
//	# dml057: bug 384
            "dml057",
//	# dml061: bug 574, 575
//	#dml069 - Bug 577
//	# Bug 628
            "dml073",
//	# Bug 601
            "dml075",
//	#dml079
//	# dml081 - divide by zero
            "dml081",
//	# dml083 - NULL in column where Max(column)
            "dml083",
            "dml087",
//	# dml090 - Bug 601
            "dml091",
            "dml104",
            "dml106",
            "dml112",
            "dml114",
            "dml119",
            "dml132",
            "dml144",
            "dml147",
            "dml148",
//	#dml149: bug 499
            "dml149",
            "dml155",
            "dml158",
            "dml162",
//	# dml168: bug 500
            "dml168",
//	#dml170: Bug 456, bug 501
            "dml170",
            "xts701",
//	#xts729 - Bug 562
//	#xts742 - Bug 630
            "xts742",
//	# xts752: alter table add constraint
            "xts752",
            "yts797",
            "yts798",
//	#yts811 - Bug 592
            "yts811",
//	# yts799: bug 494
            "yts799",
            "drop");

}
