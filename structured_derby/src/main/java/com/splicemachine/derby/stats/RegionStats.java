package com.splicemachine.derby.stats;

import static com.splicemachine.derby.stats.TimeUtils.toSeconds;

import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Statistics gatherer for coprocessor-exec stages. Collects statistics from
 * Coprocessor executions, and stores it for formatting/display purposes.
 *
 * @author Scott Fines
 * Created on: 2/26/13
 */
public class RegionStats {
	private static Logger LOG = Logger.getLogger(RegionStats.class);
	
    private final Map<byte[], Stats> processStats = Maps.newConcurrentMap();
    private final Map<byte[], Stats> sinkStats = Maps.newConcurrentMap();

    private String currentOp;
    
    private long start;
    private long totalTimeTaken;
    //processed and sunk records may be different in duplicate cases?
    private long totalProcessedRecords = 0l;
    private long totalSunkRecords = 0l;
    private long maxRegionTime = 0l;
    private long minRegionTime = 0l;

    public RegionStats() {
    }
    
    public RegionStats(String currentOpName) {
    	this.currentOp = currentOpName;
    }
    
    public void addRegionStats(byte[] region, SinkStats stats){
        this.processStats.put(region,stats.getProcessStats());
        this.sinkStats.put(region,stats.getSinkStats());
    }

    public void start(){
        start = System.nanoTime();
    }

    public void finish(){
        totalTimeTaken = System.nanoTime()-start;
    }

    public long getTotalTimeTaken(){
        return totalTimeTaken;
    }
    
    public long getTotalTimeTakenMs(){
        return Math.round(totalTimeTaken/1000*1000d);
    }
    
    public long getTotalProcessedRecords(){
        return totalProcessedRecords;
    }
    
    public long getTotalSunkRecords(){
        return totalSunkRecords;
    }
    
    public long getMaxRegionTime(){
        return maxRegionTime;
    }
    
    public long getMinRegionTime(){
        return minRegionTime;
    }
    
    public long getTotalRegions() {
    	return processStats.size(); //do processStats and sinkStats have the same # of regions? 
    }

    public void recordStats(Logger log) {
        /*
         * This will emit two sets of log messages: Summary and details.
         * The Summary level stats are: how many regions were involved, the total
         * time taken in sink, then
         * the following stats at the region level:
         *  total time | median time| avg time | max time | min time | std dev. time | p75 | p95 | p99 | slowest region | fastest region
         *  total records | median records | avg records | max records | min records | std. dev records| p75 |p95 |p99 | smallest region | largest region
         *
         *  The detail level will log out the statistics for each region
         */
        boolean showDetail = log.isTraceEnabled();
        boolean showSummary = log.isDebugEnabled();
        if(!showSummary) return; //nothing to do, we don't want to record stats
        if(processStats.size()<=0){
            log.debug("No Regions reported statistics");
        }
        String sb = new StringBuilder()
                .append("Coprocessor Time: ").append(toSeconds(totalTimeTaken))
                .append("\t Number of Regions: ").append(processStats.size())
                .append("\nProcess Summary:\n")
                .append(writeSummaryStats(processStats, false))
                .append("\nSink Summary:\n")
                .append(writeSummaryStats(sinkStats, true)).toString();
        log.debug(sb);

        if(!showDetail) return; //no more to log

        StringBuilder detailBuilder = new StringBuilder().append("\nProcess Details");
        for(byte[] region:processStats.keySet()){
            detailBuilder = detailBuilder.append("\n").append(Bytes.toString(region))
                    .append("|").append(processStats.get(region));
        }
        detailBuilder = detailBuilder.append("\nSink Details");
        for(byte[] region:sinkStats.keySet()){
            detailBuilder = detailBuilder.append("\n").append(Bytes.toString(region))
                    .append("|").append(sinkStats.get(region));
        }
        log.trace(detailBuilder.toString());

    }

    private String writeSummaryStats(Map<byte[], Stats> statsMap, boolean isSunk) {
        long[] times = new long[statsMap.size()];
        long[] records = new long[statsMap.size()];

        byte[] smallestRegion = null;
        byte[] largestRegion = null;
        byte[] fastestRegion = null;
        byte[] slowestRegion = null;
        long minRecords = Long.MAX_VALUE;
        long maxRecords = 0l;
        long totalTime = 0l;
        long totalRecords = 0l;
        int pos=0;
        for(byte[] region:statsMap.keySet()){
            Stats stats = statsMap.get(region);
            long regionTotalTime= stats.getTotalTime();
            long regionTotalRecords = stats.getTotalRecords();

            if(minRegionTime > regionTotalTime){
                minRegionTime = regionTotalTime;
                fastestRegion = region;
            }
            if(maxRegionTime < regionTotalTime){
                maxRegionTime = regionTotalTime;
                slowestRegion = region;
            }
            if(minRecords > regionTotalRecords){
                minRecords = regionTotalRecords;
                smallestRegion = region;
            }
            if(maxRecords < regionTotalRecords){
                maxRecords = regionTotalRecords;
                largestRegion = region;
            }
            
            LOG.info(">>>region="+region+",regionTotalTime="+regionTotalTime+",regionTotalRecords="+regionTotalRecords);

            totalTime+=regionTotalTime;
            totalRecords += regionTotalRecords;
            times[pos] = regionTotalTime;
            records[pos] = regionTotalRecords;
            pos++;
            
            LOG.info(">>>region="+region+",totalTime="+regionTotalTime+",totalRecords="+regionTotalRecords);
        }
        
        if (isSunk)
        	this.totalSunkRecords = totalRecords;
        else
        	this.totalProcessedRecords = totalRecords;
        
        Arrays.sort(times);
        Arrays.sort(records);

        return new StringBuilder("Current operation: ").append(currentOp).append(", Total Time: ").append(toSeconds(totalTime))
                .append("\t").append("Number of Records: ").append(totalRecords)
                .append("\nTiming Stats")
                .append("\tmin: ").append(toSeconds(minRegionTime))
                .append(" |max: ").append(toSeconds(maxRegionTime))
                .append(" |med: ").append(toSeconds(times[times.length / 2]))
                .append(" |p75: ").append(toSeconds(times[3 * times.length / 4]))
                .append(" |p95: ").append(toSeconds(times[19 * times.length / 20]))
                .append(" |p99: ").append(toSeconds(times[99 * times.length / 100]))
                .append("\n")
                .append("\tfastest region: ").append(Bytes.toString(fastestRegion))
                .append("|slowest region: ").append(Bytes.toString(slowestRegion))
                .append("\nRecord Stats")
                .append("\tmin: ").append(minRecords)
                .append(" |max: ").append(maxRecords)
                .append(" |med: ").append(records[records.length/2])
                .append(" |p75: ").append(records[3*records.length/4])
                .append(" |p95: ").append(records[19*records.length/20])
                .append(" |p99: ").append(records[99*records.length/100])
                .append("\n")
                .append("\tlargest region: ").append(Bytes.toString(largestRegion))
                .append("|smallest region: ").append(Bytes.toString(smallestRegion))
                .toString();
    }
}
