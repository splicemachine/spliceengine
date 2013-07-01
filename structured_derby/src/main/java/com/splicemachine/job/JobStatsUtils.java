package com.splicemachine.job;

import com.google.common.collect.Maps;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TaskStats;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;
import com.yammer.metrics.stats.UniformSample;

import org.apache.log4j.Logger;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public class JobStatsUtils {
    private static final Logger JOB_STATS_LOGGER = Logger.getLogger("splice_statistics");

    public static void logStats(JobStats stats){
        if(JOB_STATS_LOGGER.isDebugEnabled()){
            String summaryInfo = getSummaryString(stats.getJobName(),stats);
            JOB_STATS_LOGGER.debug(summaryInfo);
        }
        if(JOB_STATS_LOGGER.isTraceEnabled()){
            JOB_STATS_LOGGER.trace(getDetails(stats.getTaskStats()));
        }
    }

    public static void logTaskStats(String scanLabel,TaskStats finish) {
        if(JOB_STATS_LOGGER.isDebugEnabled()){
            String summaryInfo = getSummaryString("scan-"+scanLabel,finish);
            JOB_STATS_LOGGER.debug(summaryInfo);
        }
        if(JOB_STATS_LOGGER.isTraceEnabled()){
            Map<String,TaskStats> stats = Maps.newHashMapWithExpectedSize(1);
            stats.put("scan-"+scanLabel,finish);
            JOB_STATS_LOGGER.trace(getDetails(stats));
        }
    }

    private static String getDetails(Map<String, TaskStats> taskStats) {
        StringBuilder sb = new StringBuilder("Task Details (taskId\trecords|max|min|p50|p75|p95|p98|p99|p999):\n");

        StringBuilder readBuilder = new StringBuilder("Read Time:");
        StringBuilder writeBuilder = new StringBuilder("Write Time:");

        for(String taskId:taskStats.keySet()){
            TaskStats taskStat = taskStats.get(taskId);
            Stats readStats = taskStat.getReadStats();
            Stats writeStats = taskStat.getWriteStats();

           readBuilder = readBuilder.append("\n\t")
                    .append(taskId).append("\t")
                    .append(readStats.getTotalRecords())
                    .append("|").append(toMicros(readStats.getMaxTime()))
                    .append("|").append(toMicros(readStats.getMinTime()))
                    .append("|").append(toMicros(readStats.getMedian()))
                    .append("|").append(toMicros(readStats.get75P()))
                    .append("|").append(toMicros(readStats.get95P()))
                    .append("|").append(toMicros(readStats.get98P()))
                    .append("|").append(toMicros(readStats.get99P()))
                    .append("|").append(toMicros(readStats.get999P()));
            writeBuilder = writeBuilder.append("\n\t")
                    .append(taskId).append("\t")
                    .append(writeStats.getTotalRecords())
                    .append("|").append(toMicros(writeStats.getMaxTime()))
                    .append("|").append(toMicros(writeStats.getMinTime()))
                    .append("|").append(toMicros(writeStats.getMedian()))
                    .append("|").append(toMicros(writeStats.get75P()))
                    .append("|").append(toMicros(writeStats.get95P()))
                    .append("|").append(toMicros(writeStats.get98P()))
                    .append("|").append(toMicros(writeStats.get99P()))
                    .append("|").append(toMicros(writeStats.get999P()));
        }
        sb = sb.append(readBuilder);
        sb = sb.append("\n");
        sb = sb.append(writeBuilder);
        return sb.toString();
    }

    private static String getSummaryString(String jobName,TaskStats stats){
        return new StringBuilder("Job ").append(jobName).append(":")
                .append("\n\t").append("Total Time: ").append(toMicros(stats.getTotalTime()))
                .append("\nTask Summary (max|min|p50|p75|p95|p98|p99|p999)")
                .append("\n\t").append(getTaskSummary(stats)).toString();
    }

    private static String getSummaryString(String jobName,JobStats stats){
        StringBuilder sb = new StringBuilder("Job ").append(jobName).append(":")
                .append("\n\t").append("Total Time: ").append(toMicros(stats.getTotalTime()))
                .append("\n\t").append("Total Tasks: ").append(stats.getNumTasks())
                .append("\n\t").append("Completed Tasks: ").append(stats.getNumCompletedTasks())
                .append("\n\t").append("Failed Tasks: ").append(stats.getNumFailedTasks())
                .append("\n\t").append("Invalidated Tasks: ").append(stats.getNumInvalidatedTasks())
                .append("\n\t").append("Cancelled Tasks: ").append(stats.getNumCancelledTasks())
                .append("\nTask Summary (max|min|p50|p75|p95|p98|p99|p999)");

        Collection<TaskStats> statValues = stats.getTaskStats().values();
        TaskStats[] statsArray = new TaskStats[statValues.size()];
        statValues.toArray(statsArray);

        return sb.append("\n\t").append(getTaskSummary(statsArray)).toString();
    }

    private static String getTaskSummary(TaskStats... allTaskStats){
        long minTotalTime = Long.MAX_VALUE;
        long maxTotalTime = 0l;
        long minReadTime = Long.MAX_VALUE;
        long maxReadTime = 0l;
        long minWriteTime = Long.MAX_VALUE;
        long maxWriteTime = 0l;
        long minRecordsRead = Long.MAX_VALUE;
        long minRecordsWritten = Long.MAX_VALUE;
        long maxRecordsRead = 0l;
        long maxRecordsWritten = 0l;

        Sample totalTimeSummarySample = new UniformSample(allTaskStats.length);
        Sample readTimeSummarySample = new UniformSample(allTaskStats.length);
        Sample writeTimeSummarySample = new UniformSample(allTaskStats.length);
        Sample readRecordsSummarySample = new UniformSample(allTaskStats.length);
        Sample writeRecordsSummarySample = new UniformSample(allTaskStats.length);

        for(TaskStats stats:allTaskStats){
            Stats readStats = stats.getReadStats();
            Stats writeStats = stats.getWriteStats();
            long totalTime = stats.getTotalTime();
            if(maxTotalTime<totalTime)
                maxTotalTime = totalTime;
            if(minTotalTime > totalTime)
                minTotalTime = totalTime;
            if(maxReadTime<totalTime)
                maxReadTime = totalTime;
            if(minReadTime > totalTime)
                minReadTime = totalTime;
            if(maxWriteTime<totalTime)
                maxWriteTime = totalTime;
            if(minWriteTime > totalTime)
                minWriteTime = totalTime;

            if(minRecordsRead > readStats.getTotalRecords())
                minRecordsRead = readStats.getTotalRecords();
            if(maxRecordsRead < readStats.getTotalRecords())
                maxRecordsRead = readStats.getTotalRecords();
            if(minRecordsWritten > writeStats.getTotalRecords())
                minRecordsWritten = writeStats.getTotalRecords();
            if(maxRecordsWritten < writeStats.getTotalRecords())
                maxRecordsWritten = writeStats.getTotalRecords();


            totalTimeSummarySample.update(totalTime);
            readTimeSummarySample.update(stats.getReadStats().getTotalTime());
            writeTimeSummarySample.update(stats.getWriteStats().getTotalTime());
            readRecordsSummarySample.update(stats.getReadStats().getTotalRecords());
            writeRecordsSummarySample.update(stats.getWriteStats().getTotalRecords());
        }

        Snapshot totalTimeSummary = totalTimeSummarySample.getSnapshot();
        Snapshot readTimeSummary = readTimeSummarySample.getSnapshot();
        Snapshot writeTimeSummary = writeTimeSummarySample.getSnapshot();
        Snapshot readRecordsSummary = readRecordsSummarySample.getSnapshot();
        Snapshot writeRecordsSummary = writeRecordsSummarySample.getSnapshot();

        StringBuilder sb = new StringBuilder();

        snapshotToString("Total Time",minTotalTime, maxTotalTime, totalTimeSummary, sb,true);
        sb.append("\n\t");
        snapshotToString("Read Time", minReadTime, maxReadTime, readTimeSummary, sb,true);
        sb.append("\n\t");
        snapshotToString("Write Time", minWriteTime, maxWriteTime, writeTimeSummary, sb,true);
        sb.append("\n\t");
        snapshotToString("Records Read", minRecordsRead, maxRecordsRead, readRecordsSummary, sb,false);
        sb.append("\n\t");
        snapshotToString("Records Written", minRecordsWritten, maxRecordsWritten, writeRecordsSummary, sb,false);

        return sb.toString();
    }

    private static void snapshotToString(String label,
                                         long min,
                                         long max,
                                         Snapshot summary,
                                         StringBuilder sb,
                                         boolean toMicros) {
        sb.append(label).append(":\t");
        if(toMicros){
            sb.append(toMicros(max))
                    .append("|").append(toMicros(min))
                    .append("|").append(toMicros(summary.getMedian()))
                    .append("|").append(toMicros(summary.get75thPercentile()))
                    .append("|").append(toMicros(summary.get95thPercentile()))
                    .append("|").append(toMicros(summary.get98thPercentile()))
                    .append("|").append(toMicros(summary.get99thPercentile()))
                    .append("|").append(toMicros(summary.get999thPercentile()));
        }else{
            sb.append(max)
                    .append("|").append(min)
                    .append("|").append(summary.getMedian())
                    .append("|").append(summary.get75thPercentile())
                    .append("|").append(summary.get95thPercentile())
                    .append("|").append(summary.get98thPercentile())
                    .append("|").append(summary.get99thPercentile())
                    .append("|").append(summary.get999thPercentile());

        }
    }

    private static double toMicros(long nanos){
        return nanos/1000.0d;
    }

    private static double toMicros(double nanos){
        return nanos/1000;
    }

}
