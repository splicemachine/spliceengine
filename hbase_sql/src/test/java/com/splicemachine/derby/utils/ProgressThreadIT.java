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
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class ProgressThreadIT {
    @Test
    public void testSerDeProgressInfo() {
        ProgressInfo pi = new ProgressInfo("Hello, World!", 148, 22, 24, 89043, 390, 60);
        String s = pi.serializeToString();
        ProgressInfo p2 = ProgressInfo.deserializeFromString(s);
        Assert.assertTrue( pi.equals(p2) );
    }
    @Test
    public void testProgressThread() throws UnsupportedEncodingException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        PrintStream ps = new PrintStream(baos, true, utf8);

        ProgressThread p = new ProgressThread( null, ps );
        p.updateProgress( new ProgressInfo("Preparing", 0, 0, 2, 0, 4, 100).toString() );
        Assert.assertEquals( "Preparing (Job 1, Stage 1 of 2)\n", baos.toString());

        p.updateProgress( new ProgressInfo("Preparing", 0, 0, 2, 20, 4, 100).toString() );
        Assert.assertEquals( "Preparing (Job 1, Stage 1 of 2)\n[------------+------", baos.toString());

        p.updateProgress("Preparing\n0 0 2 40 4 100");
        Assert.assertEquals("Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+--", baos.toString());
        p.updateProgress("Preparing\n0 0 2 60 4 100");
        Assert.assertEquals("Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%--------", baos.toString());
        p.updateProgress("Preparing\n0 0 2 80 4 100");
        Assert.assertEquals("Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%---", baos.toString());
        p.updateProgress("Preparing\n0 0 2 100 4 100");
        Assert.assertEquals("Preparing (Job 1, Stage 1 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]", baos.toString());

        baos.reset();
        p.updateProgress("Calculate Result\n0 1 2 20 4 100");
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+------", baos.toString());
        p.updateProgress("Calculate Result\n0 1 2 40 4 100");
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+----------25%----------+--", baos.toString());

        p.updateProgress("Listing\n1 0 3 0 4 10");
        Assert.assertEquals("\nCalculate Result (Job 1, Stage 2 of 2)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Listing (Job 2, Stage 1 of 3)\n", baos.toString());
        baos.reset();
        p.updateProgress("Step Three\n2 0 1 3 4 10");
        Assert.assertEquals("" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step Three (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%---", baos.toString());

        p.finishCurrentProgressBar();

        Assert.assertEquals("" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step Three (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n",
                    baos.toString());
    }

    @Test
    public void testProgressThread2() throws UnsupportedEncodingException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        PrintStream ps = new PrintStream(baos, true, utf8);

        ProgressThread p = new ProgressThread( null, ps );

        // intentionally skipping one job (e.g. job is finished so fast we didn't catch that
        p.updateProgress("Step ONE\n0 0 1 20 4 100");
        p.updateProgress("Step ONE\n0 0 1 25 4 100");
        p.updateProgress("Step TWO\n2 0 1 20 4 100");
        p.updateProgress("Step THREE\n3 0 1 20 4 100");
        p.updateProgress("Step THREE\n3 0 1 100 4 100");
        Assert.assertEquals(
                "Step ONE (Job 1, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step TWO (Job 3, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]\n" +
                "Step THREE (Job 4, Stage 1 of 1)\n" +
                "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]", baos.toString());

    }
}
