/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.system.oe.client;

/**
 * Execute transactions using multiple threads.
 * A single thread uses a single submitter,
 * submitters are created outside of this class.
 */
public class MultiThreadSubmitter {

    /**
     * Execute count transactions per submitter
     * using a newly created thread for each
     * submitter. In total (count*submitter.length)
     * transactions will be executed. The time returned
     * will be the time to execute all the transactions.
     * 
     * Each submitter will have its clearTransactionCount called
     * before the run.
     * 
     * @param submitters Submitters to use.
     * @param displays Displays for each submitter.
     * If null then null will be passed into each transaction
     * execution
     * @param count Number of transactions per thread.
     * @return Time to excute all of the transactions.
     */
    public static long multiRun(
            Submitter[] submitters,
            Object[] displays,
            int count) {

        Thread[] threads = new Thread[submitters.length];
        for (int i = 0; i < submitters.length; i++) {
            submitters[i].clearTransactionCount();
            Object displayData = displays == null ? null : displays[i];
            threads[i] = newThread(i, submitters[i], displayData, count);
        }

        // Start all the threads
        long start = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++)
            threads[i].start();

        // and then wait for them to finish
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long end = System.currentTimeMillis();

        return end - start;
    }

    /**
     * Return a thread that will run count transactions using a submitter.
     * 
     * @param threadId
     *            Number of thread.
     * @param submitter
     *            Submitter
     * @param displayData
     *            DisplayData for this submitter
     * @param count
     *            Number of transactions to run.
     * 
     * @return Thread (not started)
     */
    private static Thread newThread(final int threadId,
            final Submitter submitter,
            final Object displayData, final int count) {
        Thread t = new Thread("OE_Thread:" + threadId) {

            public void run() {
                try {
                    submitter.runTransactions(displayData, count);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        return t;
    }

}
