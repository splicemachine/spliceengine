/*
 * Copyright (c) 2020 - 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.db.shared.ProgressInfo;
import com.splicemachine.db.impl.tools.ij.ProgressThread;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/*
example for "small jobs", execute this in sqlshell

drop table a;
create table a(i int);
insert into a values 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
insert into a select i+10 from a;
insert into a select i+20 from a;
insert into a select i+40 from a;
insert into a select i+80 from a;
insert into a select i+160 from a;
insert into a select i+320 from a;
insert into a select i+640 from a;
insert into a select i+1280 from a;
insert into a select i+2560 from a;
insert into a select i+5120 from a;
insert into a select i+10000 from a;
insert into a select i+20000 from a;
insert into a select i+40000 from a;
insert into a select i+80000 from a;
insert into a select i+160000 from a;
insert into a select i+320000 from a;
insert into a select i+640000 from a;
insert into a select i+1280000 from a;
insert into a select i+2560000 from a;
insert into a select i+5120000 from a;
insert into a select i+10240000 from a; -- will show progress bar

select count(*) from a; -- will show progress bar

-- this will result in one normal progress bar, and then one with "small jobs"
select * from a inner join (select count(*) as i from a ) b --splice-properties joinStrategy=broadcast
 on a.i = b.i inner join (select count(*) as i from a ) c --splice-properties joinStrategy=broadcast
 on a.i = c.i;
*/
public class ProgressThreadTest {
    @Test
    public void testSerDeProgressInfo() {
        ProgressInfo pi = new ProgressInfo("Hello,\nWorld!", 148, 22, 24, 89043, 390, 60);
        String s = pi.serializeToString();
        ProgressInfo p2 = ProgressInfo.deserializeFromString(s);
        Assert.assertTrue(pi.equals(p2));
        Assert.assertEquals(pi.hashCode(), p2.hashCode());
        Assert.assertEquals(pi.getJobname(), "Hello,\nWorld!");
        Assert.assertEquals(pi.getJobNumber(), 148);
    }
    @Test
    public void testProgressThread() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        PrintStream ps = new PrintStream(baos, true, utf8);

        ProgressThread p = new ProgressThread( null, ps );
        String r = p.getFirstRunning() + "\n";
        p.updateProgress( new ProgressInfo("Preparing", 0, 0, 2, 0, 4, 100).toString() );
        Assert.assertEquals( r + "Preparing (Job 1, Stage 1 of 2)\n", baos.toString());

        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 20, 4, 100).toString() );
        Assert.assertEquals( r + "Preparing (Job 1, Stage 1 of 2)\n[------------+------", baos.toString());

        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 40, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+--", baos.toString());
        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 60, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%--------", baos.toString());


        // ignore if progress goes backward
        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 50, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%--------", baos.toString());

        // ignore invalid information (
        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 90, -15, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%--------", baos.toString());


        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 80, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%---", baos.toString());


        // progress 100%
        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 100, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]", baos.toString());

        // don't overshoot progressbar
        p.updateProgress(new ProgressInfo("Preparing", 0, 0, 2, 120, 4, 100));
        Assert.assertEquals(r + "Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]", baos.toString());



        baos.reset();
        p.updateProgress(new ProgressInfo("Calculate Result", 0, 1, 2, 20, 4, 100));
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+------", baos.toString());
        p.updateProgress(new ProgressInfo("Calculate Result", 0, 1, 2, 40, 4, 100));
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+----------25%----------+--", baos.toString());

        p.updateProgress(new ProgressInfo("Listing", 1, 0, 3, 0, 4, 10));
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Listing (Job 2, Stage 1 of 3)\n", baos.toString());
        baos.reset();
        p.updateProgress(new ProgressInfo("Step Three", 2, 0, 1, 3, 4, 10));
        Assert.assertEquals("" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step Three (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%---", baos.toString());

        p.updateProgress(new ProgressInfo("SMALL\nJOB", 4, 0, 1, 0, 1, 2));

        Assert.assertEquals("" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step Three (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "SMALL\n" +
                "JOB (Collection of Jobs)\n",
                    baos.toString());
        baos.reset();

        // sometimes we get a number of small jobs that only have two jobs each. combine them in one progressbar.
        // see comment beginning of file
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 4, 0, 1, 0, 2, 2));
        Assert.assertEquals("", baos.toString());

        p.updateProgress(new ProgressInfo("SMALL\nJOB", 5, 0, 1, 0, 1, 2));
        Assert.assertEquals("[", baos.toString());
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 6, 0, 1, 0, 1, 2));
        Assert.assertEquals("[-", baos.toString());
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 7, 0, 1, 0, 1, 2));
        Assert.assertEquals("[--", baos.toString());
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 8, 0, 1, 0, 1, 1));
        Assert.assertEquals("[---", baos.toString());
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 9, 0, 1, 0, 2, 2));
        Assert.assertEquals("[----", baos.toString());
        p.updateProgress(new ProgressInfo("SMALL\nJOB", 10, 0, 1, 1, 2, 2));
        Assert.assertEquals("[-----", baos.toString());

        // switch back to non-small jobs progress bar if we get jobs with > 2 Tasks

        p.updateProgress(new ProgressInfo("Another Result", 12, 1, 2, 20, 4, 100));
        Assert.assertEquals(
                        "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                        "Another Result (Job 13, Stage 2 of 2)\n" +
                        "[------------+------", baos.toString());

        p.updateProgress(new ProgressInfo("Another Result", 12, 1, 2, 60, 4, 100));
        Assert.assertEquals(
                        "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                        "Another Result (Job 13, Stage 2 of 2)\n" +
                        "[------------+----------25%----------+-----------50%--------", baos.toString());

        // don't repeat job name if it doesn't change
        p.updateProgress(new ProgressInfo("Another Result", 13, 1, 2, 2, 1, 3));
        Assert.assertEquals(
                        "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                        "Another Result (Job 13, Stage 2 of 2)\n" +
                        "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                        "(Job 14, Stage 2 of 2)\n" +
                        "[------------+----------25%----------+-----------50%-----------+---", baos.toString());

    }

    @Test
    public void testProgressThread2() throws UnsupportedEncodingException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        PrintStream ps = new PrintStream(baos, true, utf8);

        ProgressThread p = new ProgressThread( null, ps );
        String r = p.getFirstRunning() + "\n";

        // intentionally skipping one job (e.g. job is finished so fast we didn't catch that
        p.updateProgress(new ProgressInfo("Step ONE", 0, 0, 1, 20, 4, 100));
        p.updateProgress(new ProgressInfo("Step TWO", 0, 0, 1, 25, 4, 100));
        p.updateProgress(new ProgressInfo("Step TWO", 2, 0, 1, 20, 4, 100));
        p.updateProgress(new ProgressInfo("Step THREE", 3, 0, 1, 20, 4, 100));
        p.updateProgress(new ProgressInfo("Step THREE", 3, 0, 1, 100, 4, 100));
        Assert.assertEquals(
                r + "Step ONE (Job 1, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step TWO (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step THREE (Job 4, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]", baos.toString());

    }
}
