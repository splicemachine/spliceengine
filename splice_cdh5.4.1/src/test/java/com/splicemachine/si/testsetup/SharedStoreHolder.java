package com.splicemachine.si.testsetup;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.HTransactorFactory;

import java.util.concurrent.TimeUnit;

/**
 * Used by multiple SI unit tests so that HStoreSetup only starts an HBase mini-cluster once.
 */
public class SharedStoreHolder {

    private static HStoreSetup hStoreSetup;
    private static TestTransactionSetup testTransactionSetup;

    private static boolean started;

    public synchronized static HStoreSetup getHstoreSetup() {
        if (!started) {
            start();
        }
        return hStoreSetup;
    }

    public synchronized static TestTransactionSetup getTestTransactionSetup() {
        if (!started) {
            start();
        }
        return testTransactionSetup;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // private
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private synchronized static void start() {
        try {
            SpliceConstants.numRetries = 1;
            SIConstants.committingPause = 1000;
            hStoreSetup = new HStoreSetup();
            testTransactionSetup = new TestTransactionSetup(hStoreSetup, false);
            HTransactorFactory.setTransactor(testTransactionSetup.hTransactor);
            started = true;

            /* The HbaseMiniCluster will shutdown by itself when the JVM exists when you are running a single SI test
             * or the entire set of SI unit tests ('mvn test').  Here we just start a thread to automatically shut
             * it down when the JVM stays alive (to run ITs say). This probably isn't strictly necessary. */
            new ShutdownThread(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2)).start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized static void shutdown() {
        try {
            hStoreSetup.shutdown();
            started = false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class ShutdownThread extends Thread {

        private final long shutdownTime;

        private ShutdownThread(long shutdownTime) {
            this.shutdownTime = shutdownTime;
        }

        @Override
        public void run() {
            while (System.currentTimeMillis() < shutdownTime) {
                Thread.yield();
            }
            shutdown();
        }
    }

}
