/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.concurrent.Threads;

import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Waits for connections on a given host+port to be available.
 */
public class SpliceTestPlatformWait {


    public static boolean wait(String hostname, int port, long waitSec) {
        long startTime = System.currentTimeMillis();
        long elapsedSecs = 0;
        while (elapsedSecs < waitSec) {
            try {
                new Socket(hostname, port);
                System.out.println("\nStarted\n");
                break;
            } catch (Exception e) {
                System.out.println(format("SpliceTestPlatformWait: Not started, still waiting for '%s:%s'. %s of %s seconds elapsed.",
                        hostname, port, elapsedSecs, waitSec));
                Threads.sleep(1, TimeUnit.SECONDS);
            }
            elapsedSecs = (long) ((System.currentTimeMillis() - startTime) / 1000d);
        }
        if (elapsedSecs >= waitSec) {
            System.out.println(format("Waited %s seconds without success", waitSec));
            return false;
        }
        return true;
    }

    /**
     * argument 0 - hostname
     * argument 1 - port
     */
    public static void main(String[] arguments) {

        String hostname = arguments[0];
        int port = Integer.valueOf(arguments[1]);


        if (!wait(hostname, port, 360L)) {
            System.exit(-1);
        }

    }

}
