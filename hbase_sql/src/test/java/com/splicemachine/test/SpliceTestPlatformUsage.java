/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.test;

import java.io.PrintStream;

class SpliceTestPlatformUsage {

    public static void usage(String msg, Throwable t) {
        PrintStream out = System.out;
        if (t != null) {
            out = System.err;
        }
        if (msg != null) {
            out.println(msg);
        }
        if (t != null) {
            t.printStackTrace(out);
        }
        out.println("Usage: String hbaseRootDirUri, Integer masterPort, " +
                "Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort, Integer derbyPort, String true|false");

        System.exit(1);
    }
}
