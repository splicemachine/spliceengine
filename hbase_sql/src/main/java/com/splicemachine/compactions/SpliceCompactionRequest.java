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

package com.splicemachine.compactions;

import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension of CompactionRequest with a hook to block scans while Storefile's are being renamed
 * Created by dgomezferro on 3/24/16.
 */
public class SpliceCompactionRequest extends CompactionRequest {
    private static final Logger LOG = Logger.getLogger(SpliceCompactionRequest.class);
    private AtomicReference<MemstoreAware> memstoreAware;

    public void preStorefilesRename() throws IOException {
        assert memstoreAware != null;
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if (latest.currentScannerCount>0) {
                SpliceLogUtils.warn(LOG,"compaction Delayed waiting for scanners to complete scannersRemaining=%d",latest.currentScannerCount);
                try {
                    Thread.sleep(1000); // Have Split sleep for a second
                } catch (InterruptedException e1) {
                    throw new IOException(e1);
                }
                continue;
            }
            if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementCompactionCount(latest)))
                break;
        }
    }

    @Override
    public void afterExecute(){
        if (memstoreAware == null) {
            // memstoreAware hasn't been set, the compaction failed before it could block and increment the counter, so don't do anything
            return;
        }
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if (memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest)))
                break;
        }
    }

    public void setMemstoreAware(AtomicReference<MemstoreAware> memstoreAware) {
        this.memstoreAware = memstoreAware;
    }
}
