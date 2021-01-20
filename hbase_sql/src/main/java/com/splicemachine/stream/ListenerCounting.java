package com.splicemachine.stream;

import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.apache.spark.status.api.v1.StageData;
import scala.collection.JavaConverters;

import java.util.List;

// https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/ui/ConsoleProgressBar.scala
// https://queirozf.com/entries/apache-spark-architecture-overview-clusters-jobs-stages-tasks
public class ListenerCounting extends SparkListener implements AutoCloseable
{
    OperationContext context;
    SparkContext sc;
    String uuid;
    String sql;
    OlapStatus status;

    // see https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/status/AppStatusListener.scala
    // https://wanghao989711.gitbooks.io/spark-2-translation/content/spark-SparkListener.html
    // https://books.japila.pl/apache-spark-internals/apache-spark-internals/2.4.4/webui/spark-webui-JobProgressListener.html
    public ListenerCounting(OperationContext context, String uuid, String sql, OlapStatus status) {
//            this.sc = SparkContext.getOrCreate( SpliceSpark.getSession().sparkContext().getConf() );
        //this.sc = SparkContext.getOrCreate( SpliceSpark.getContext().getConf() );
        this.sql = sql;
        this.sc = SpliceSpark.getSession().sparkContext();
        this.uuid = uuid;
        this.sc.getLocalProperties().setProperty("operation-uuid", uuid);

        sc.addSparkListener(this);
        log("build listenercounting " + this + "\n");
        this.context = context;
        this.status = status;
    }
    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        numStages++;

        log("Stage completed " + numStages + " / " + totalNumStages + "\n");
        super.onStageCompleted(stageCompleted);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        log("Stage submitted " + numStages + " / " + totalNumStages + "\n");
        super.onStageSubmitted(stageSubmitted);
    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        super.onTaskGettingResult(taskGettingResult);
    }

    int numStages = 0;
    int totalNumStages = 0;
    int numJobs = 0;
    String jobName = "";
    List<StageInfo> toWatch;
    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        if (jobStart.properties().getProperty("operation-uuid", "").equals(uuid)) {
            toWatch = JavaConverters.seqAsJavaListConverter(jobStart.stageInfos()).asJava();
        }
        else
        {
//                log("JOB NOT INTERESTING FOR US!\n");
            return;
        }

        totalNumStages = 0;
        numStages = 0;
        for(StageInfo info : toWatch)
        {
            totalNumStages++;
        }
        numJobs++;
        super.onJobStart(jobStart);
        log(" job with " + totalNumStages + " stages\n" );
        String callSite = jobStart.properties().getProperty("callSite.short");
        String jobDesc = jobStart.properties().getProperty("spark.job.description");
        jobName = callSite + ": " + jobDesc;
        log( callSite + "\n");
        log( jobDesc + "\n");
        String s = " " + numJobs + " " + numStages + " 0 0 1";
        status.setString( jobName + "\n" + s );
        log("\n" + jobName + "\n" + s + "\n");
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        log(" job ended\n" );
        super.onJobEnd(jobEnd);
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        super.onEnvironmentUpdate(environmentUpdate);
    }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        super.onBlockManagerAdded(blockManagerAdded);
    }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        super.onBlockManagerRemoved(blockManagerRemoved);
    }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        super.onUnpersistRDD(unpersistRDD);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log(" onApplicationStart\n" );
        super.onApplicationStart(applicationStart);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log(" onApplicationEnd\n" );
        super.onApplicationEnd(applicationEnd);
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
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        super.onExecutorRemoved(executorRemoved);
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        super.onExecutorBlacklisted(executorBlacklisted);
    }

    @Override
    public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
        super.onExecutorBlacklistedForStage(executorBlacklistedForStage);
    }

    @Override
    public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
        super.onNodeBlacklistedForStage(nodeBlacklistedForStage);
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        super.onExecutorUnblacklisted(executorUnblacklisted);
    }

    @Override
    public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
        super.onNodeBlacklisted(nodeBlacklisted);
    }

    @Override
    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
        super.onNodeUnblacklisted(nodeUnblacklisted);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);
    }

    @Override
    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
        super.onSpeculativeTaskSubmitted(speculativeTask);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        super.onOtherEvent(event);
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        //log( this + " Starting: " + numStages + " / " + totalNumStages + "\n");

    }

    void log(String s)
    {
        try { String name = ""; java.io.FileOutputStream fos = new java.io.FileOutputStream("/tmp/log.txt", true);
            fos.write((sql + ": " + s).getBytes()); fos.close(); } catch( Exception e) {}
    }
    long lastUpdate = 0;
    long updateDelay = 250;


    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
//            if( toWatch.stream().map( stageInfo -> stageInfo.stageId() ).anyMatch( id -> id == taskEnd.stageId() ) == false )
//            {
//                return;
//            }
        long now = System.currentTimeMillis();
        if( now < lastUpdate + 1) //updateDelay)
            return;
        lastUpdate = now;

        if(context != null) context.recordPipelineWrites( taskEnd.taskMetrics().outputMetrics().recordsWritten() );
        if(context != null) context.recordRead( taskEnd.taskMetrics().inputMetrics().recordsRead());
//            log(taskEnd.taskMetrics().inputMetrics().toString() + "\n");

        StringBuilder sb = new StringBuilder();
        for(StageData sd : JavaConverters.seqAsJavaListConverter(sc.statusStore().activeStages()).asJava())
        {
            int total = sd.numTasks();
            if( toWatch.stream().map( stageInfo -> stageInfo.stageId() ).anyMatch( id -> id == sd.stageId() ) == false ) {
//                    log("DOES NOT CONTAIN " + taskEnd.stageId() + "\n");
                continue;
            }
            if(false) {
                String header = "[ Job " + numJobs + ", Stage " + (numStages + 1) + " / " + totalNumStages + " ";
                String tailer = " (" + sd.numCompleteTasks() + " + " + sd.numActiveTasks() + ") / " + sd.numTasks() + " ]";
                sb.append(header);
                int width = 70;
                int w = width - header.length() - tailer.length();
                if (w > 0) {
                    int percent = w * sd.numCompleteTasks() / total;
                    for (int i = 0; i < w; i++) {
                        if (i < percent)
                            sb.append('=');
                        else if (i == percent)
                            sb.append('>');
                        else
                            sb.append('-');
                    }
                }
                sb.append(tailer);
            }
            else {
                String s = " " + numJobs + " " + numStages + " " + sd.numCompleteTasks() + " " + sd.numActiveTasks() + " " + sd.numTasks();
                sb.append(s);
            }
        }
        status.setString( jobName + "\n" + sb.toString() );
        sb.append("\n");
        log(sb.toString());

//            log( " records Written: " + taskEnd.taskMetrics().outputMetrics().recordsWritten() + "\n");
//            log( " bytes Written: " + taskEnd.taskMetrics().outputMetrics().bytesWritten() + "\n");
//            log( " bytes Read: " + taskEnd.taskMetrics().inputMetrics().bytesRead() + "\n");
//            log( " records Read: " + taskEnd.taskMetrics().inputMetrics().recordsRead() + "\n");
    }

    @Override
    public void close() {
        sc.removeSparkListener(this);
    }
}
