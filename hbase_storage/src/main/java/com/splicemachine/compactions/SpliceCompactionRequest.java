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

package com.splicemachine.compactions;

import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.si.impl.server.PurgeConfig;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension of CompactionRequest with a hook to block scans while Storefile's are being renamed
 * Created by dgomezferro on 3/24/16.
 */
public class SpliceCompactionRequest extends CompactionRequestImpl {
    private static final Logger LOG = Logger.getLogger(SpliceCompactionRequest.class);
    private AtomicReference<MemstoreAware> memstoreAware;
    private HRegion region;
    private boolean compactionCountIncremented = false;

    private PurgeConfig purgeConfig = null;

    public SpliceCompactionRequest(Collection<HStoreFile> files) {
        super(files);
    }

    public void preStorefilesRename() throws IOException {
        assert memstoreAware != null;
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            assert latest.currentCompactionCount >= 0;
            if (latest.currentScannerCount>0) {
                SpliceLogUtils.warn(LOG,"compaction Delayed waiting for scanners to complete scannersRemaining=%d",latest.currentScannerCount);
                try {
                    Thread.sleep(1000); // Have Split sleep for a second
                } catch (InterruptedException e1) {
                    throw new IOException(e1);
                }
                continue;
            }
            if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementCompactionCount(latest))) {
                if(LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "memstoreAware@" + System.identityHashCode(memstoreAware) +
                            " 's compactionCount incremented from " + latest.currentCompactionCount +
                            " to " + (latest.currentCompactionCount + 1));
                }
                assert !compactionCountIncremented;
                compactionCountIncremented = true;
                break;
            }
        }
    }
    public void afterExecute(){
        if (memstoreAware == null || !compactionCountIncremented) {
            // memstoreAware hasn't been set, the compaction failed before it could block and increment the counter, so don't do anything
            return;
        }
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            assert latest.currentCompactionCount > 0;
            if (memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest))) {
                if(LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "memstoreAware@" + System.identityHashCode(latest) +
                            " 's compactionCount decremented from " + latest.currentCompactionCount +
                            " to " + (latest.currentCompactionCount - 1));
                }
                compactionCountIncremented = false;
                break;
            }
        }
        if(region != null) {
            try {
                region.getCoprocessorHost().postCompact(null, null, null, null, null);
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

    @Override
    public void setOffPeak(boolean value) {
        // We hijack setOffPeak because it is only called twice:
        // 1. set to true in SpliceDefaultCompactionPolicy before compaction happens
        // 2. set to false in HStore.finishCompactionRequest (hbase code)
        // At those points, the value passed is irrelevant and is "only" used for logging
        // purpose, so we can hijack it.
        super.setOffPeak(value);
        if (!value) {
            afterExecute();
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public PurgeConfig getPurgeConfig() {
        assert purgeConfig != null;
        return purgeConfig;
    }

    public void setPurgeConfig(PurgeConfig purgeConfig) {
        this.purgeConfig = purgeConfig;
    }
}
