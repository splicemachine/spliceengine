package com.splicemachine.stream;

import com.splicemachine.db.impl.tools.ij.ProgressThread;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.apache.spark.status.api.v1.StageData;
import scala.collection.JavaConverters;

import java.util.List;

public class ListenerCounting extends SparkListener implements AutoCloseable
{
    SparkContext sc;
    String uuid;
    String sql;
    OlapStatus status;
    int stagesCompleted = 0;
    int numStages = 0;
    int jobNumber = 0;
    String jobName = "";
    List<StageInfo> toWatch;

    void log(String s)
    {
        try { String name = ""; java.io.FileOutputStream fos = new java.io.FileOutputStream("/tmp/log.txt", true);
            fos.write((sql + ": " + s).getBytes()); fos.close(); } catch( Exception e) {}
    }

    public ListenerCounting(String uuid, String sql, OlapStatus status) {
        this.sql = sql;
        this.sc = SpliceSpark.getSession().sparkContext();
        this.uuid = uuid;
        this.sc.getLocalProperties().setProperty("operation-uuid", uuid);

        sc.addSparkListener(this);
        this.status = status;
        log("build listenercounting " + this + "\n");
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
        log( jobName + "\n");
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
        log(" job ended\n" );
    }


    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
    }

    @Override
    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
        super.onStageExecutorMetrics(executorMetrics);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        super.onOtherEvent(event);
    }

    void updateProgress() {
        StringBuilder sb = new StringBuilder();
        for(StageData sd : JavaConverters.seqAsJavaListConverter(sc.statusStore().activeStages()).asJava())
        {
            if(toWatch.stream().map(StageInfo::stageId).anyMatch(id -> id == sd.stageId())) {
                ProgressThread.ProgressInfo pi = new ProgressThread.ProgressInfo(jobName, jobNumber,
                        stagesCompleted, numStages, sd.numCompleteTasks(), sd.numActiveTasks(), sd.numTasks());
                pi.toString(sb);
                break; // stage found
            }
        }
        status.setString(sb.toString() );
        sb.append("\n");
        log(sb.toString());
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        if(toWatch.stream().map(StageInfo::stageId).anyMatch(id -> id == taskStart.stageId() ) )
            updateProgress();
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if(toWatch.stream().map(StageInfo::stageId).anyMatch(id -> id == taskEnd.stageId() ) )
            updateProgress();
    }

    @Override
    public void close() {
        sc.removeSparkListener(this);
    }
}
