package com.splicemachine.test.nist.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.test.diff.DiffEngine;
import com.splicemachine.test.nist.NistTestUtils;
import com.splicemachine.test.runner.DerbyRunner;
import com.splicemachine.test.runner.SpliceRunner;
import com.splicemachine.test.utils.DependencyTree;
import com.splicemachine.test.utils.TestUtils;
import com.splicemachine.test.verify.VerifyReport;

/**
 * TODO: Temporary - for framework testing
 * @author Jeff Cunningham
 *         Date: 7/25/13
 */
@Ignore
public class ShortNistIT {

    private static List<String> derbyOutputFilter;
    private static List<String> spliceOutputFilter;

    private static DerbyRunner derbyRunner;
    private static SpliceRunner spliceRunner;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Read in the bug filters for output files
        derbyOutputFilter = NistTestUtils.readDerbyFilters();
        spliceOutputFilter = NistTestUtils.readSpliceFilters();

        derbyRunner = new DerbyRunner(NistTestUtils.TARGET_NIST_DIR);
        spliceRunner = new SpliceRunner(NistTestUtils.TARGET_NIST_DIR);
    }

    @Test
    @Ignore
    public void runTests() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        // need two tests run cause 2nd depends on schema creation in 1st
        List<File> testFiles = new ArrayList<File>();
        testFiles.add(new File(TestUtils.getResourceDirectory(), "/nist/schema8.sql"));
        testFiles.add(new File(TestUtils.getResourceDirectory(), "/nist/cdr002.sql"));
        TestUtils.runTests(testFiles, Arrays.asList(derbyRunner, spliceRunner), ps);
        // diff output and assert no differences in each report
        DiffEngine theDiffer = new DiffEngine(TestUtils.getBaseDirectory()+NistTestUtils.TARGET_NIST_DIR, derbyOutputFilter,spliceOutputFilter);
        Collection<VerifyReport> reports = theDiffer.verifyOutput(testFiles);

        Map<String,Integer> failedDiffs = VerifyReport.Report.reportCollection(reports, ps);

        System.out.print(baos.toString("UTF-8"));

        Assert.assertEquals(failedDiffs.size() +  " tests had differences: "+failedDiffs,
                reports.size(), (reports.size() - failedDiffs.size()));
    }
    
    @Test
    @Ignore
    public void testDeleteSchemaDependencies() throws Exception {
        SpliceRunner spliceRunner = new SpliceRunner(NistTestUtils.TARGET_NIST_DIR, null);
//        NistTestUtils.runTests(NistTestUtils.createRunList("schema1.sql"),
//                new DerbyNistRunner(), NistTestUtils.readDerbyFilters(),
//                spliceRunner, NistTestUtils.readSpliceFilters(), System.out);

        String schema = "FLATER";
        Connection connection = spliceRunner.getConnection();
        DependencyTree tree = TestUtils.getTablesAndViews(connection, schema, System.out);
        List<DependencyTree.DependencyNode> depOrder = tree.getDependencyOrder();
        System.out.println(depOrder.size() + " Nodes to delete");
        for (DependencyTree.DependencyNode node : depOrder) {
            System.out.println(node.name+" <"+node.type+"> Deps: "+tree.resolveNodeNames(node.depIDs)+
                    " Parents: "+tree.resolveNodeNames(node.parentIDs));
        }

        TestUtils.dropTableOrView(connection, schema, depOrder, System.out);
    }

    @Test
    @Ignore
    public void testDeleteSchemas() throws Exception {
        SpliceRunner spliceRunner = new SpliceRunner(NistTestUtils.TARGET_NIST_DIR);
        DerbyRunner derbyRunner = new DerbyRunner(NistTestUtils.TARGET_NIST_DIR);
        TestUtils.runTests(NistTestUtils.createRunList("schema1.sql"),
                Arrays.asList(derbyRunner, spliceRunner),
                System.out);

        TestUtils.cleanup(Arrays.asList(derbyRunner, spliceRunner), System.out);
    }

}
