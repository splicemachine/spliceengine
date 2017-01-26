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

package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class PipelineUtils{
    private static final Logger LOG=Logger.getLogger(PipelineUtils.class);
    public static final int RETRY_BACKOFF[] = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};
    private static final Random RANDOM = new Random();
    public static final PreFlushHook noOpFlushHook = new PreFlushHook() {
        @Override
        public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception {
            return new ArrayList<>(buffer);
        }
    };

    public static Collection<KVPair> doPartialRetry(BulkWrite bulkWrite,BulkWriteResult response,long id) throws Exception{
        IntOpenHashSet notRunRows=response.getNotRunRows();
        IntObjectOpenHashMap<WriteResult> failedRows=response.getFailedRows();
        Collection<KVPair> toRetry=new ArrayList<>(failedRows.size()+notRunRows.size());
        List<String> errorMsgs= Lists.newArrayListWithCapacity(failedRows.size());
        int i=0;
        Collection<KVPair> allWrites=bulkWrite.getMutations();
        for(KVPair kvPair : allWrites){
            if(notRunRows.contains(i))
                toRetry.add(kvPair);
            else{
                WriteResult writeResult=failedRows.get(i);
                if(writeResult!=null){
                    errorMsgs.add(writeResult.getErrorMessage());
                    if(writeResult.canRetry())
                        toRetry.add(kvPair);
                }
            }
            i++;
        }
        if(LOG.isTraceEnabled()){
            int[] errorCounts=new int[11];
            for(IntObjectCursor<WriteResult> failedCursor : failedRows){
                errorCounts[failedCursor.value.getCode().ordinal()]++;
            }
            SpliceLogUtils.trace(LOG,"[%d] %d failures with types: %s",id,failedRows.size(),Arrays.toString(errorCounts));
        }

        return toRetry;
    }


    /**
     *
     * Get Pause time in millis (Mirrors Hbase)
     *
     * @param pause
     * @param tries
     * @return
     */

    public static long getPauseTime(final int tries, final long pause) {
        int ntries = tries;
        if (ntries >= RETRY_BACKOFF.length) {
            ntries = RETRY_BACKOFF.length - 1;
        }
        long normalPause = pause * RETRY_BACKOFF[ntries];
        long jitter =  (long)(normalPause * RANDOM.nextFloat() * 0.01f); // 1% possible jitter
        return normalPause + jitter;
    }

}