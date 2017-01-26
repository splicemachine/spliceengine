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

package com.splicemachine.olap;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.compactions.CompactionInputFormat;
import com.splicemachine.compactions.CompactionResult;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkFlatMapFunction;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ui.jobs.UIData;
import scala.Option;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class CompactionJob implements Callable<Void>{

    private static final Logger LOG = Logger.getLogger(CompactionJob.class);

    private final OlapStatus status;
    private final DistributedCompaction compactionRequest;

    private final Clock clock;
    private final long tickTime;

    public CompactionJob(DistributedCompaction compactionRequest,
                         OlapStatus jobStatus,
                         Clock clock,
                         long tickTime) {
        this.status = jobStatus;
        this.clock = clock;
        this.tickTime = tickTime;
        this.compactionRequest = compactionRequest;
    }

    @Override
    public Void call() throws Exception {
        if(!status.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }
        initializeJob();
        Configuration conf = new Configuration(HConfiguration.unwrapDelegate());
        if (LOG.isTraceEnabled()) {
            LOG.trace("regionLocation = " + compactionRequest.regionLocation);
        }
        conf.set(MRConstants.REGION_LOCATION, compactionRequest.regionLocation);
        conf.set(MRConstants.COMPACTION_FILES,getCompactionFilesBase64String());

        SpliceSpark.pushScope(compactionRequest.scope+": Parallelize");
        //JavaRDD rdd1 = SpliceSpark.getContext().parallelize(files, 1);
        //ParallelCollectionRDD rdd1 = getCompactionRDD();

        JavaSparkContext context=SpliceSpark.getContext();
        JavaPairRDD<Integer, Iterator> rdd1=context.newAPIHadoopRDD(conf,
                CompactionInputFormat.class,
                Integer.class,
                Iterator.class);
        rdd1.setName("Distribute Compaction Load");
        SpliceSpark.popScope();

        SpliceSpark.pushScope(compactionRequest.scope + ": Compact files");
        JavaRDD<String> rdd2=rdd1.mapPartitions(new SparkFlatMapFunction<>(compactionRequest.compactionFunction));
        rdd2.setName(compactionRequest.jobDetails);
        SpliceSpark.popScope();

        SpliceSpark.pushScope("Compaction");
        if(!status.isRunning()){
            //the client timed out during our setup, so it's time to stop
            return null;
        }
        long startTime = clock.currentTimeMillis();
        JavaFutureAction<List<String>> collectFuture=rdd2.collectAsync();
        while(!collectFuture.isDone()){
            try{
                collectFuture.get(tickTime,TimeUnit.MILLISECONDS);
            }catch(TimeoutException te){
                /*
                 * A TimeoutException just means that tickTime expired. That's okay, we just stick our
                 * head up and make sure that the client is still operating
                 */
            }
            if(!status.isRunning()){
                /*
                 * The client timed out, so cancel the compaction and terminate
                 */
                collectFuture.cancel(true);
                context.cancelJobGroup(compactionRequest.jobGroup);
                return null;
            }
            if (clock.currentTimeMillis() - startTime > compactionRequest.maxWait) {
                // Make sure compaction is scheduled in Spark and running, otherwise cancel it and fallback to in-HBase compaction
                if (!compactionRunning(collectFuture.jobIds())) {
                    collectFuture.cancel(true);
                    context.cancelJobGroup(compactionRequest.jobGroup);
                    status.markCompleted(new FailedOlapResult(
                            new RejectedExecutionException("No resources available for running compaction in Spark")));
                    return null;
                }
            }
        }
        //the compaction completed
        List<String> sPaths = collectFuture.get();
        status.markCompleted(new CompactionResult(sPaths));
        SpliceSpark.popScope();

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Paths Returned: %s",sPaths);
        return null;
    }

    private boolean compactionRunning(List<Integer> jobIds) {
        if (jobIds.size() == 0) {
            return false;
        }
        Integer jobId = jobIds.get(0);
        Option<UIData.JobUIData> op = SpliceSpark.getContext().sc().jobProgressListener().jobIdToData().get(jobId);
        if (op.isEmpty()) {
            return false;
        }
        UIData.JobUIData jobData = op.get();
        if (jobData.numActiveTasks() > 0) {
            return true;
        }
        if (jobData.numCompletedTasks() > 0) {
            return true;
        }
        return false;
    }

    protected void initializeJob() {
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setJobGroup(compactionRequest.jobGroup,compactionRequest.jobDescription);
        dsp.setSchedulerPool(compactionRequest.poolName);
    }

    private String getCompactionFilesBase64String() throws IOException, StandardException{
        return compactionRequest.base64EncodedFileList();
    }
}
