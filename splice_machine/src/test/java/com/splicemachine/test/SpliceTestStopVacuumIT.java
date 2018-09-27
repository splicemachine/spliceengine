package com.splicemachine.test;

import org.junit.Test;

public class SpliceTestStopVacuumIT {
    @Test
    public void stopVacuum() throws Exception {
        System.out.println("Stopping Vaccum process ...");
        String[] cmd = {
                "/bin/bash",
                "-c",
                "ps -ef | grep spliceVacuum | grep -v grep | awk '{print $2}' | xargs kill"
        };
        Process p = Runtime.getRuntime().exec(cmd);
        Thread.sleep(120000);
        int result = p.waitFor();
        System.out.println("Stopped Vaccum process , return code " + Integer.toString(result));
    }
}
