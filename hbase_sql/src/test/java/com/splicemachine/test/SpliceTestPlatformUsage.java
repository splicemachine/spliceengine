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
