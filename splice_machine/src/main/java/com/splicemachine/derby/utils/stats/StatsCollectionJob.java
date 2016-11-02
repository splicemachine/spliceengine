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

package com.splicemachine.derby.utils.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.StatisticsFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class StatsCollectionJob implements Callable<Void> {
    private static final Logger LOG = Logger.getLogger(StatsCollectionJob.class);
    private final DistributedStatsCollection request;
    private final OlapStatus jobStatus;

    public StatsCollectionJob(DistributedStatsCollection request, OlapStatus jobStatus) {
        this.request = request;
        this.jobStatus = jobStatus;
    }

    @Override
    public Void call() throws Exception {
        if (!jobStatus.markRunning()) {
            //the client has already cancelled us or has died before we could get started, so stop now
            LOG.error("Client bailed out");
            return null;
        }
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.jobGroup, "");
        try {


            DataSet statsDataSet;
            if (request.scanSetBuilder.getStoredAs() !=null) {
                ScanSetBuilder builder = request.scanSetBuilder;
                String storedAs = request.scanSetBuilder.getStoredAs();
                    if (storedAs.equals("T"))
                        statsDataSet = dsp.readTextFile(null,builder.getLocation(),builder.getDelimited(),null,builder.getColumnPositionMap(),null,builder.getTemplate());
                    else if (storedAs.equals("P"))
                        statsDataSet = dsp.readParquetFile(builder.getColumnPositionMap(),builder.getLocation(),null,null,null,builder.getTemplate());
                    else if (storedAs.equals("O"))
                        statsDataSet =  dsp.readORCFile(builder.getColumnPositionMap(),builder.getLocation(),null,null,null,builder.getTemplate());
                    else {
                        throw new UnsupportedOperationException("storedAs Type not supported -> " + storedAs);
                    }
            }
            else {
                statsDataSet = request.scanSetBuilder.buildDataSet(request.scope);
            }

            List<LocatedRow> result = statsDataSet
                    .mapPartitions(
                    new StatisticsFlatMapFunction(request.scanSetBuilder.getBaseTableConglomId(),request.scanSetBuilder.getColumnPositionMap(), request.scanSetBuilder.getTemplate())).collect();
            jobStatus.markCompleted(new StatsResult(result));
            return null;
        } catch (Exception e) {
            LOG.error("Oops", e);
            throw e;
        }
    }
}
