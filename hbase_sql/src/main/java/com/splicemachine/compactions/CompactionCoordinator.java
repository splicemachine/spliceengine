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
 *
 */

package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.olap.DistributedCompaction;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Coordinator for initiating compactions. This logic is shared by all versions of HBase
 * compactors.
 *
 * @author Scott Fines
 *         Date: 12/8/16
 */
public class CompactionCoordinator{
    private final Logger LOG = Logger.getLogger(this.getClass());

    private long smallestReadPoint;
    private String conglomId = null;
    private String tableDisplayName = null;
    private String indexDisplayName = null;
    private String jobDetails = null;

    private static final String TABLE_DISPLAY_NAME_ATTR = SIConstants.TABLE_DISPLAY_NAME_ATTR;
    private static final String INDEX_DISPLAY_NAME_ATTR = SIConstants.INDEX_DISPLAY_NAME_ATTR;

    private final Store store;
    private final long mat;

    public CompactionCoordinator(long mat,Store store){
        this.store=store;
        this.mat = mat;

        conglomId = this.store.getTableName().getQualifierAsString();
        tableDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(TABLE_DISPLAY_NAME_ATTR);
        indexDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(INDEX_DISPLAY_NAME_ATTR);
    }

    public List<Path> compact(CompactionContext context) throws IOException,RejectedExecutionException {
        smallestReadPoint = context.getSmallestReadPoint();
        List<String> files = context.getFilePaths();

        String regionLocation = getRegionLocation(store);
        SConfiguration conf =HConfiguration.getConfiguration();
        DistributedCompaction jobRequest=new DistributedCompaction(
                getCompactionFunction(mat),
                files,
                getJobDetails(context),
                getJobGroup(context,regionLocation),
                getJobDescription(context),
                getPoolName(),
                getScope(context),
                regionLocation,
                conf.getOlapCompactionMaximumWait());
        CompactionResult result = null;
        Future<CompactionResult> futureResult = EngineDriver.driver().getOlapClient().submit(jobRequest);
        while(result == null) {
            try {
                result = futureResult.get(conf.getOlapClientTickTime(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException e) {
                Throwable cause=e.getCause();
                if (cause instanceof RejectedExecutionException) {
                    throw (RejectedExecutionException)cause;
                }
                throw Exceptions.rawIOException(cause);
            } catch (TimeoutException e) {
                // check region write status
                if (!store.areWritesEnabled()) {
                    futureResult.cancel(true);
                    context.cancel();
                    // TODO should we cleanup files written by Spark?
                    throw new IOException("Region has been closed, compaction aborted");
                }
            }
        }

        List<String> sPaths = result.getPaths();

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Paths Returned: %s", sPaths);

        context.complete();

        List<Path> paths = new ArrayList<>();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }
        return paths;

    }

    /* ***************************************************************************************************************/
    /*private helper methods*/

    private SparkCompactionFunction getCompactionFunction(long matToUse) {
        return new SparkCompactionFunction(
                smallestReadPoint,
                store.getTableName().getNamespace(),
                store.getTableName().getQualifier(),
                store.getRegionInfo(),
                store.getFamily().getName(),
                matToUse);
    }

    private String getJobDetails(CompactionContext context) {
        if (jobDetails == null) {
            String delim=",\n";
            jobDetails =getTableInfoLabel(delim) +delim
                    +String.format("Region Name=%s",this.store.getRegionInfo().getRegionNameAsString()) +delim
                    +String.format("Region Id=%d",this.store.getRegionInfo().getRegionId()) +delim
                    +String.format("File Count=%d",context.getFilePaths().size()) +delim
                    +String.format("Total File Size=%s",FileUtils.byteCountToDisplaySize(context.fileSize())) +delim
                    +String.format("Type=%s",getMajorMinorLabel(context));
        }
        return jobDetails;
    }

    private String getJobGroup(CompactionContext request,String regionLocation) {
        return regionLocation+":"+Long.toString(request.getSelectionTime());
    }

    private String getPoolName() {
        return "compaction";
    }

    private String getJobDescription(CompactionContext request) {
        int size = request.getFilePaths().size();
        return String.format("%s Compaction: %s, %d %s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "),
                size,
                (size > 1 ? "Files" : "File"));
    }

    private String getMajorMinorLabel(CompactionContext request) {
        return request.isMajor() ? "Major" : "Minor";
    }

    private String getTableInfoLabel(String delim) {
        StringBuilder sb = new StringBuilder();
        if (indexDisplayName != null) {
            sb.append(String.format("Index=%s", indexDisplayName));
            sb.append(delim);
        } else if (tableDisplayName != null) {
            sb.append(String.format("Table=%s", tableDisplayName));
            sb.append(delim);
        }
        sb.append(String.format("Conglomerate=%s", conglomId));
        return sb.toString();
    }

    private String getScope(CompactionContext request) {
        return String.format("%s Compaction: %s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "));
    }

    /**
     * Returns location for an HBase store
     * @param store
     * @return
     * @throws java.io.IOException
     */
    private String getRegionLocation(Store store) throws IOException {
        // Get start key for the store
        HRegionInfo regionInfo = store.getRegionInfo();
        byte[] startKey = regionInfo.getStartKey();

        // Get an instance of the table
        PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
        Partition table = tableFactory.getTable(regionInfo.getTable());

        // Get region location using start key
        List<Partition> partitions = table.subPartitions(startKey, HConstants.EMPTY_END_ROW, true);
        if (partitions.isEmpty()) {
            throw new IOException("Couldn't find region location for " + regionInfo);
        }
        return partitions.get(0).owningServer().getHostname();
    }

}
