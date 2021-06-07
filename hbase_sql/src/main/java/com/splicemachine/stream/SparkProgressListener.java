/*
 * Copyright (c) 2020 - 2021 Splice Machine, Inc.
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

package com.splicemachine.stream;

import com.splicemachine.db.shared.ProgressInfo;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.apache.spark.status.api.v1.StageData;
import scala.collection.JavaConverters;

import java.util.List;

/**
 * A SparkListener that sends progress information to the OlapStatus of the operation,
 * which will send this within a OlapMessage.ProgressResponse to the RegionServer, which
 * will provide this information then in SYSCS_GET_PROGRESS / sqlshell progress bar.
 * Make sure to close resource so that SparkListener is removed from spark context.
 */
public class SparkProgressListener extends SparkListener implements AutoCloseable
{
    SparkContext sparkContext;
    String uuid;
    OlapStatus status;
    int stagesCompleted = 0;
    int numStages = 0;
    int jobNumber = 0;
    String jobName = "";
    List<StageInfo> toWatch;

    /**
     * @param uuid an UUID to identify "our" jobs. Can be UUID.randomUUID().toString()
     * @param status OlapStatus to send the progress information to
     */
    public SparkProgressListener(String uuid, OlapStatus status) {
        this.uuid = uuid;
        this.status = status;
        sparkContext = SpliceSpark.getSession().sparkContext();

        sparkContext.getLocalProperties().setProperty("operation-uuid", uuid);
        sparkContext.addSparkListener(this);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        super.onJobStart(jobStart);
        if (!jobStart.properties().getProperty("operation-uuid", "").equals(uuid)) {
            return; // not our job
        }
        toWatch = JavaConverters.seqAsJavaListConverter(jobStart.stageInfos()).asJava();
        numStages = toWatch.size();
        stagesCompleted = 0;
        jobNumber++;

        String callSite = jobStart.properties().getProperty("callSite.short");
        String jobDesc = jobStart.properties().getProperty("spark.job.description");
        jobName = callSite + ": " + jobDesc;
    }

    boolean isWatchedStageId(int stageId) {
        return toWatch.stream().map(StageInfo::stageId).anyMatch(id -> id == stageId );
    }

    void updateProgress(int updatedStageId) {
        if(!isWatchedStageId(updatedStageId)) return;

        for(StageData sd : JavaConverters.seqAsJavaListConverter(sparkContext.statusStore().activeStages()).asJava())
        {
            if(updatedStageId == sd.stageId()) {
                ProgressInfo pi = new ProgressInfo(jobName, jobNumber,
                        stagesCompleted, numStages, sd.numCompleteTasks(), sd.numActiveTasks(), sd.numTasks());
                status.setProgress(pi);
                break; // stageId found
            }
        }

    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        if(!isWatchedStageId(stageCompleted.stageInfo().stageId() )) return;
        stagesCompleted++;
    }



    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        updateProgress(taskStart.stageId());
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        updateProgress(taskEnd.stageId());
    }

    @Override
    public void close() {
        sparkContext.removeSparkListener(this);
    }
}
