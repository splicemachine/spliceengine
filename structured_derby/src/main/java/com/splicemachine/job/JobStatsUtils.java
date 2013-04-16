package com.splicemachine.job;

import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TaskStats;
import org.apache.hadoop.hbase.metrics.histogram.Sample;
import org.apache.hadoop.hbase.metrics.histogram.Snapshot;
import org.apache.hadoop.hbase.metrics.histogram.UniformSample;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public class JobStatsUtils {

    public static void logStats(JobStats stats, Logger logger){
        if(logger.isDebugEnabled()){
            String summaryInfo = getSummaryString(stats.getJobName(),stats);
            logger.debug(summaryInfo);
        }
        if(logger.isTraceEnabled()){
            logger.trace(getDetails(stats.getTaskStats()));
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
                    .append("|").append(readStats.getMaxTime())
                    .append("|").append(readStats.getMinTime())
                    .append("|").append(readStats.getMedian())
                    .append("|").append(readStats.get75P())
                    .append("|").append(readStats.get95P())
                    .append("|").append(readStats.get98P())
                    .append("|").append(readStats.get99P())
                    .append("|").append(readStats.get999P());
            writeBuilder = writeBuilder.append("\n\t")
                    .append(taskId).append("\t")
                    .append(writeStats.getTotalRecords())
                    .append("|").append(writeStats.getMaxTime())
                    .append("|").append(writeStats.getMinTime())
                    .append("|").append(writeStats.getMedian())
                    .append("|").append(writeStats.get75P())
                    .append("|").append(writeStats.get95P())
                    .append("|").append(writeStats.get98P())
                    .append("|").append(writeStats.get99P())
                    .append("|").append(writeStats.get999P());
        }
        sb = sb.append(readBuilder);
        sb = sb.append("\n");
        sb = sb.append(writeBuilder);
        return sb.toString();
    }

    private static String getSummaryString(String jobName,JobStats stats){
        return new StringBuilder("Job ").append(jobName).append(":")
                .append("\n\t").append("Total Time: ").append(stats.getTotalTime())
                .append("\n\t").append("Total Tasks: ").append(stats.getNumTasks())
                .append("\n\t").append("Completed Tasks: ").append(stats.getNumCompletedTasks())
                .append("\n\t").append("Failed Tasks: ").append(stats.getNumFailedTasks())
                .append("\n\t").append("Invalidated Tasks: ").append(stats.getNumInvalidatedTasks())
                .append("\n\t").append("Cancelled Tasks: ").append(stats.getNumCancelledTasks())
                .append("\nTask Summary (max|min|p50|p75|p95|p98|p99|p999)")
                .append("\n\t").append(getTaskSummary(stats.getTaskStats().values())).toString();
    }

    private static String getTaskSummary(Collection<TaskStats> allTaskStats){
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

        Sample totalTimeSummarySample = new UniformSample(allTaskStats.size());
        Sample readTimeSummarySample = new UniformSample(allTaskStats.size());
        Sample writeTimeSummarySample = new UniformSample(allTaskStats.size());
        Sample readRecordsSummarySample = new UniformSample(allTaskStats.size());
        Sample writeRecordsSummarySample = new UniformSample(allTaskStats.size());

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

        snapshotToString("Total Time",minTotalTime, maxTotalTime, totalTimeSummary, sb);
        sb.append("\n\t");
        snapshotToString("Read Time", minReadTime, maxReadTime, readTimeSummary, sb);
        sb.append("\n\t");
        snapshotToString("Write Time", minWriteTime, maxWriteTime, writeTimeSummary, sb);
        sb.append("\n\t");
        snapshotToString("Records Read", minRecordsRead, maxRecordsRead, readRecordsSummary, sb);
        sb.append("\n\t");
        snapshotToString("Records Written", minRecordsWritten, maxRecordsWritten, writeRecordsSummary, sb);

        return sb.toString();
    }

    private static void snapshotToString(String label,
                                         long min,
                                         long max,
                                         Snapshot summary, StringBuilder sb) {
        sb.append(label).append(":\t")
        .append(max)
        .append("|").append(min)
        .append("|").append(summary.getMedian())
        .append("|").append(summary.get75thPercentile())
        .append("|").append(summary.get95thPercentile())
        .append("|").append(summary.get98thPercentile())
        .append("|").append(summary.get99thPercentile())
        .append("|").append(summary.get999thPercentile());
    }

}
