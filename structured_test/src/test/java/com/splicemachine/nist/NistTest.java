package com.splicemachine.nist;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Jeff Cunningham
 *         Date: 7/22/13
 */
public class NistTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        BaseNistTest.loadFilteredFiles();
    }

    @Test
    public void runNistTest() throws Exception {
        System.out.println("Starting...");

        // run derby
        System.out.println("Derby...");
        DerbyNistTest.setup();
        long start = System.currentTimeMillis();
        DerbyNistTest.createSchema();
        System.out.println("    Schema creation and load: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        start = System.currentTimeMillis();
        DerbyNistTest.runDerby();
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // run splice
        System.out.println("Splice...");
        SpliceNistTest.setup();
        start = System.currentTimeMillis();
        SpliceNistTest.createSchema();
        System.out.println("    Schema creation and load: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        start = System.currentTimeMillis();
        SpliceNistTest.runSplice();
        System.out.println("    Tests: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // diff output
        Collection<BaseNistTest.DiffReport> reports = BaseNistTest.diffOutput(new ArrayList<String>());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        boolean success = true;
        for (BaseNistTest.DiffReport report : reports) {
            report.print(ps);
            success = success && report.isEmpty();
        }

        Assert.assertTrue(reports.size() +" Tests failed"+baos.toString("UTF-8")+"\n"+reports.size()+" Tests had differencs.", success);
    }
}
