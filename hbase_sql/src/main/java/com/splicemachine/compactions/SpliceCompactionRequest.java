/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.compactions;

import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
    private HRegion region;

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
        if(region != null) {
            try {
                region.getCoprocessorHost().postCompact(null, null, null);
            } catch (Exception e) {
                throw new RuntimeException("Error running postCompact hook");
            }
        }
    }

    public void setMemstoreAware(AtomicReference<MemstoreAware> memstoreAware) {
        this.memstoreAware = memstoreAware;
    }

    public void setRegion(HRegion region) {
        this.region = region;
    }
}
