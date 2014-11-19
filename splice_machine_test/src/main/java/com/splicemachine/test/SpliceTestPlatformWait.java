package com.splicemachine.test;

import com.splicemachine.concurrent.Threads;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Waits for connections on a given host+port to be available.
 */
public class SpliceTestPlatformWait {

    private static final long MAX_WAIT_SECS = TimeUnit.SECONDS.toSeconds(90);

    /**
     * argument 0 - hostname
     * argument 1 - port
     */
    public static void main(String[] arguments) throws IOException {

        String hostname = arguments[0];
        int port = Integer.valueOf(arguments[1]);

        long startTime = System.currentTimeMillis();
        long elapsedSecs = 0;
        while (elapsedSecs < MAX_WAIT_SECS) {
            try {
                new Socket(hostname, port);
                System.out.println("\nStarted\n");
                break;
            } catch (Exception e) {
                System.out.println(format("SpliceTestPlatformWait: Not started, still waiting for '%s:%s'. %s of %s seconds elapsed.",
                        hostname, port, elapsedSecs, MAX_WAIT_SECS));
                Threads.sleep(1, TimeUnit.SECONDS);
            }
            elapsedSecs = (long) ((System.currentTimeMillis() - startTime) / 1000d);
        }
        if (elapsedSecs >= MAX_WAIT_SECS) {
            System.out.println(format("Waited %s seconds without success", MAX_WAIT_SECS));
            System.exit(-1);
        }

    }

}
