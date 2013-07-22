package com.splicemachine.nist;

import org.junit.Assert;
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

    @Test
    public void runNistTest() throws Exception {
        System.out.println("Starting...");
        // run derby
        long start = System.currentTimeMillis();
        DerbyNistTest.setup();
        System.out.println("Setting up Derby (schema creation and load) run took: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        start = System.currentTimeMillis();
        DerbyNistTest.runDerby();
        System.out.println("Running Derby test run took: " + BaseNistTest.getDuration(start, System.currentTimeMillis()));

        // run splice
        start = System.currentTimeMillis();
        SpliceNistTest.setup();
        System.out.println("Setting up Splice run took: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        start = System.currentTimeMillis();
        SpliceNistTest.runSplice();
        System.out.println("Running Derby test run took: " + BaseNistTest.getDuration(start,System.currentTimeMillis()));

        // diff output
        Collection<BaseNistTest.DiffReport> reports = BaseNistTest.diffOutput(new ArrayList<String>());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        boolean success = true;
        for (BaseNistTest.DiffReport report : reports) {
            report.print(ps);
            success = success && report.isEmpty();
        }

        Assert.assertTrue(reports.size() +" Tests failed"+baos.toString("UTF-8"), success);
    }
}
