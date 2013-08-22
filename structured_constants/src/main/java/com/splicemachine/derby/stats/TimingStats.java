package com.splicemachine.derby.stats;

import com.google.common.util.concurrent.AtomicDouble;
import com.splicemachine.constants.SpliceConstants;
import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;
import com.yammer.metrics.stats.UniformSample;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static com.splicemachine.derby.stats.TimeUtils.toSeconds;

/**
 * Throughput and Timing statistics accumulation.
 *
 * @author Scott Fines
 * Created on: 2/26/13
 */
public class TimingStats implements Stats{
    private static final long serialVersionUID = 1l;
    private static final int DEFAULT_SAMPLE_SIZE = 100;

    private long totalTime;
    private long totalRecords;
    private double avgTime;
    private double timeVariation;
    private long maxTime;
    private long minTime;
    private double[] percentiles; //includes p50,p75,p95,p98,p99,p999 in that order

    public TimingStats(){}

    public TimingStats(long totalTime, long totalRecords,
                       double avgTime, double timeVariation,
                       long maxTime, long minTable, Snapshot timingSample) {
        this.totalTime = totalTime;
        this.totalRecords = totalRecords;
        this.avgTime = avgTime;
        this.timeVariation = timeVariation;
        this.maxTime = maxTime;
        this.minTime = minTable;

        this.percentiles = new double[6];
        percentiles[0] = timingSample.getMedian();
        percentiles[1] = timingSample.get75thPercentile();
        percentiles[2] = timingSample.get95thPercentile();
        percentiles[3] = timingSample.get98thPercentile();
        percentiles[4] = timingSample.get99thPercentile();
        percentiles[5] = timingSample.get999thPercentile();
    }

    public TimingStats(long totalTime, long totalRecords){
        this.totalTime = totalTime;
        this.totalRecords = totalRecords;
        this.percentiles = new double[6];
        Arrays.fill(this.percentiles, 0.0);
    }

    /**
     * @return the total time taken to process all records.
     */
    @Override
    public long getTotalTime(){
        return this.totalTime;
    }

    /**
     * @return the total number of processed records
     */
    @Override
    public long getTotalRecords() {
        return totalRecords;
    }

    /**
     * @return the average time taken to process a single record
     */
    @Override
    public double getAvgTime() {
        return avgTime;
    }

    /**
     * @return the variation in processing time for a single record
     */
    public double getTimeVariation() {
        return timeVariation;
    }

    /**
     * @return the standard deviation in processing time for a single record.
     */
    @Override
    public double getTimeStandardDeviation(){
        return Math.sqrt(timeVariation/(totalRecords-1));
    }

    /**
     * @return the maximum processing time for a single record.
     */
    @Override
    public long getMaxTime() {
        return maxTime;
    }

    /**
     * @return the minimum processing time for a single record.
     */
    @Override
    public long getMinTime() {
        return minTime;
    }

    /**
     * @return the median time taken to process a single record.
     */
    @Override
    public double getMedian(){
        return percentiles[0];
    }

    /**
     * @return the 75th percentile time taken to process a single record. That is,
     * 75% of all records were processed in less time than this.
     */
    @Override
    public double get75P(){
        return percentiles[1];
    }

    /**
     * @return the 95th percentile time taken to process a single record. That is,
     * 95% of all records were processed in less time than this.
     */
    @Override
    public double get95P(){
        return percentiles[2];
    }

    /**
     * @return the 98th percentile time taken to process a single record. That is,
     * 98% of all records were processed in less time than this.
     */
    @Override
    public double get98P(){
        return percentiles[3];
    }

    /**
     * @return the 99th percentile time taken to process a single record. That is,
     * 99% of all records were processed in less time than this.
     */
    @Override
    public double get99P(){
        return percentiles[4];
    }

    /**
     * @return the 99.9th percentile time taken to process a single record. That is,
     * 99.9% of all records were processed in less time than this.
     */
    @Override
    public double get999P(){
        return percentiles[5];
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(totalTime);
        out.writeLong(totalRecords);
        out.writeDouble(avgTime);
        out.writeDouble(timeVariation);
        out.writeLong(maxTime);
        out.writeLong(minTime);
        out.writeInt(percentiles.length);
        for(double percentile:percentiles){
            out.writeDouble(percentile);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        totalTime = in.readLong();
        totalRecords = in.readLong();
        avgTime = in.readDouble();
        timeVariation = in.readDouble();
        maxTime = in.readLong();
        minTime = in.readLong();

        percentiles = new double[in.readInt()];
        for(int i=0;i<percentiles.length;i++){
            percentiles[i] = in.readDouble();
        }
    }

    public static com.splicemachine.derby.stats.Accumulator uniformAccumulator(){

        com.splicemachine.derby.stats.Accumulator acc;

        if(SpliceConstants.COLLECT_STATS){
            acc = new Accumulator(new UniformSample(DEFAULT_SAMPLE_SIZE));
        }else{
            acc = new RecordAccumulator();
        }

        return acc;
    }

    public static com.splicemachine.derby.stats.Accumulator uniformSafeAccumulator(){

        com.splicemachine.derby.stats.Accumulator acc;

        if(SpliceConstants.COLLECT_STATS){
            acc = new ThreadSafeAccumulator(new UniformSample(DEFAULT_SAMPLE_SIZE));
        }else{
            acc = new RecordAccumulator();
        }

        return acc;
    }

    public static com.splicemachine.derby.stats.Accumulator uniformAccumulator(int sampleSize){
        return new Accumulator(new UniformSample(sampleSize));
    }

    public static com.splicemachine.derby.stats.Accumulator exponentialAccumulator(int sampleSize, double decayWeight){
        return new Accumulator(new ExponentiallyDecayingSample(sampleSize,decayWeight));
    }

    @Override
    public String toString() {
        return String.format("records: %d|time: %.6f|avg t: %.6f|std. dev t: %.6f|med t: %.6f|75p t: %.6f|95p t: %.6f|98p t: %.6f|99p t: %.6f|999p t: %.6f",
                totalRecords,
                toSeconds(totalTime),
                toSeconds(avgTime),
                toSeconds(getTimeStandardDeviation()),
                toSeconds(getMedian()),
                toSeconds(get75P()),
                toSeconds(get95P()),
                toSeconds(get98P()),
                toSeconds(get99P()),
                toSeconds(get999P()));
    }

    public static class Accumulator implements com.splicemachine.derby.stats.Accumulator{
        private long totalTime = 0l;
        private long totalRecords = 0l;
        private double avgTime = 0l;
        private double timeVariation = 0l;
        private long maxTime = 0l;
        private long minTime = Long.MAX_VALUE;
        private Sample processSample;

        private long start;

        public Accumulator(Sample processSample) {
            this.processSample = processSample;
        }

        public void start(){
            this.start = System.nanoTime();
        }

        @Override
        public void tickRecords(long numRecords) {
            totalRecords += numRecords;
        }

        @Override
        public void tickRecords() {
            totalRecords++;
        }

        @Override
        public boolean shouldCollectStats() {
            return true;
        }

        public void tick(long numRecords,long time){
            tickRecords(numRecords);
            this.totalTime+=time;
            this.processSample.update(time);
            updateMax(time);
            updateMin(time);
            updateVariation(time);
        }

        public void tick(long time){
            tick(1l,time);
        }

        public Stats finish(){
            return new TimingStats(totalTime,totalRecords,
                    avgTime,timeVariation,maxTime,
                    minTime, processSample.getSnapshot());
        }

/*********************************************************************************/
        /*private helper methods*/
        private void updateMax(long time) {
            if(maxTime<time)
                maxTime = time;
        }

        private void updateMin(long time){
            if(minTime> time)
                minTime = time;
        }

        private void updateVariation(long time){
            double oldAvg = avgTime;
            avgTime = oldAvg+(time-oldAvg)/totalRecords;

            double oldVar = timeVariation;
            timeVariation = oldVar+(time-oldAvg)*(time-avgTime);
        }
    }

    private static class RecordAccumulator implements com.splicemachine.derby.stats.Accumulator{

        private AtomicLong totalRecords = new AtomicLong(0l);
        private AtomicLong startTime = new AtomicLong(0l);

        @Override
        public void start() {
            startTime.compareAndSet(0l,System.nanoTime());
        }

        @Override
        public Stats finish() {
            long end = System.nanoTime();
            return new TimingStats(end - startTime.get(), totalRecords.get());
        }

        @Override
        public void tick(long numRecords, long timeTaken) {
            tickRecords(numRecords);
        }

        @Override
        public void tick(long timeTaken) {
            tickRecords(1l);
        }

        @Override
        public void tickRecords(long numRecords) {
            totalRecords.addAndGet(numRecords);
        }

        @Override
        public void tickRecords() {
            totalRecords.incrementAndGet();
        }

        @Override
        public boolean shouldCollectStats() {
            return false;
        }
    }

    private static class ThreadSafeAccumulator implements com.splicemachine.derby.stats.Accumulator{
        private AtomicLong totalTime = new AtomicLong(0l);
        private AtomicLong totalRecords = new AtomicLong(0l);
        private AtomicDouble avgTime = new AtomicDouble(0d);
        private AtomicDouble timeVariation = new AtomicDouble(0d);
        private AtomicLong maxTime = new AtomicLong(0l);
        private AtomicLong minTime = new AtomicLong(Long.MAX_VALUE);
        private AtomicLong tickCount = new AtomicLong(0l);
        private Sample processSample;

        private long start;

        public ThreadSafeAccumulator(Sample processSample){
            this.processSample = processSample;
        }
        @Override
        public void start() {
            totalTime.set(0l);
            start = System.nanoTime();
        }

        @Override
        public Stats finish() {
            return new TimingStats(System.nanoTime()-start,totalRecords.get(),avgTime.get(),timeVariation.get(),
                    maxTime.get(),minTime.get(),processSample.getSnapshot());
        }

        @Override
        public void tickRecords(long numRecords) {
            totalRecords.addAndGet(numRecords);
        }

        @Override
        public void tickRecords() {
            totalRecords.incrementAndGet();
        }

        @Override
        public boolean shouldCollectStats() {
            return true;
        }

        @Override
        public void tick(long numRecords, long timeTaken) {
            tickRecords(numRecords);
            totalTime.addAndGet(timeTaken);
            updateMax(timeTaken);
            updateMin(timeTaken);
            updateAvg(timeTaken);
            processSample.update(timeTaken);
        }

        private void updateAvg(long timeTaken) {
            synchronized (ThreadSafeAccumulator.this){
                double oldAvg = avgTime.get();
                double oldVar = timeVariation.get();
                long newTickCount = tickCount.incrementAndGet();

                double newAvg = oldAvg+(timeTaken-oldAvg)/newTickCount;
                double newVar = oldVar+(timeTaken-oldAvg)*(timeTaken-newAvg);
                avgTime.set(newAvg);
                timeVariation.set(newVar);
                tickCount.set(newTickCount);
            }
        }

        @Override
        public void tick(long timeTaken) {
            tick(1l,timeTaken);
        }

        private void updateMax(long timeTaken) {
            while(true){
                long oldMax = maxTime.get();
                if(oldMax >=timeTaken||maxTime.compareAndSet(oldMax,timeTaken)) return;
            }
        }

        private void updateMin(long timeTaken){
            while(true){
                long oldMin = minTime.get();
                if(oldMin < timeTaken||minTime.compareAndSet(oldMin,timeTaken))return;
            }
        }
    }
}

