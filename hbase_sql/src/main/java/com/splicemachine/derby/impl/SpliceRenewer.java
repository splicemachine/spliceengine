/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.impl;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.SerializableWritable;
import org.apache.spark.broadcast.Broadcast;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SpliceRenewer {
    private static final Logger LOG = Logger.getLogger(SpliceRenewer.class);

    private CountDownLatch latch = new CountDownLatch(1);
    
    public void start() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Broadcast<?> oldCreds = null;
                while (true) {
                    try {
                        if (latch.await(15, TimeUnit.MINUTES)) {
                            // we've finished
                            return;
                        }
                        Credentials creds = UserGroupInformation.getCurrentUser().getCredentials();
                        if (oldCreds != null) {
                            oldCreds.destroy();
                        }
                        oldCreds = SpliceSpark.getCredentials();
                        SpliceSpark.setCredentials(SpliceSpark.getContextUnsafe().broadcast(new SerializableWritable(creds)));
                    } catch (Exception e) {
                        LOG.error("Unexpected exception", e);
                        return;
                    }
                }
            }
        }, "SpliceCredentialRenewer");
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        latch.countDown();
    }
}
