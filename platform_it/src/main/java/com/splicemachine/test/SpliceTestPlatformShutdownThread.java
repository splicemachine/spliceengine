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

import org.apache.hadoop.hbase.MiniHBaseCluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Upon any connection on port 6666 calls shutdown on MiniHBaseCluster.
 * <p/>
 * Allows one to shutdown SpliceTestPlatform from the command line.  Using netcat for example: 'nc localhost 6666'
 * <p/>
 * Why not use HbaseAdmin here?  Apparently the work necessary to cleanly shutdown a MiniHBaseCluster is a superset
 * of of what HbaseAdmin.shutdown does.  MiniHBaseCluster.shutdown() invokes the normal Hbase shutdown stuff plus
 * whatever else is unique to the MiniHBaseCluster.
 * <p/>
 * Note that this will kill all region servers if you have started multiple using our spliceClusterMember profile.
 * <p/>
 * A quick way to test that this mechanism is working:  If you comment out all coprocessor in SpliceTestPlatformConfig
 * start SpliceTestPlatform and then 'nc localhost 60000' should should see the SpliceTestPlatform completely shutdown
 * and the JVM exit in about two seconds.  It this works, but the same does not happen when the coprocessors are
 * enabled, then something in splice is keeping the JVM alive.
 */
public class SpliceTestPlatformShutdownThread extends Thread {

    private final MiniHBaseCluster miniHBaseCluster;
    private final ServerSocket serverSocket;

    public SpliceTestPlatformShutdownThread(MiniHBaseCluster miniHBaseCluster) throws IOException {
        this.miniHBaseCluster = miniHBaseCluster;
        this.serverSocket = new ServerSocket(6666);
        this.start();
    }

    @Override
    public void run() {
        try {
            serverSocket.accept();
            System.out.println("Received connection on SpliceTestPlatformShutdown port 6666, calling shutdown()");
            miniHBaseCluster.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Main method here is independent of the rest of this class.  This is just an alternative way it initialize
     * the shutdown from java.  Instead of 'nc localhost 6666' just run this main method.
     */
    public static void main(String[] arg) throws IOException {
        new Socket("localhost", 6666);
    }

}
