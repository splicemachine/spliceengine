package com.splicemachine.derby.stats;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Aggregate statistics for a Shuffle/Sink operation.
 *
 * @author Scott Fines
 * Created on: 2/26/13
 */
public class TaskStats implements Externalizable{
    private static final long serialVersionUID = 1l;

    private long totalTime;
		private long totalRowsProcessed;
		private long totalRowsWritten;

    public TaskStats(){}
    
    public TaskStats(Stats processStats, Stats sinkStats, long totalTime){
				this.totalRowsProcessed = processStats.getTotalRecords();
				this.totalRowsWritten = sinkStats.getTotalRecords();
        this.totalTime = totalTime;
    }

		public long getTotalTime() { return totalTime; }
		public long getTotalRowsProcessed() { return totalRowsProcessed; }
		public long getTotalRowsWritten() { return totalRowsWritten; }

		public TaskStats(long totalTime,long totalRowsProcessed,long totalRowsWritten){
				this.totalRowsProcessed = totalRowsProcessed;
				this.totalRowsWritten = totalRowsWritten;
				this.totalTime = totalTime;
		}

    public static SinkAccumulator uniformAccumulator(){
        return new SinkAccumulator(TimingStats.uniformAccumulator(), TimingStats.uniformAccumulator());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
				out.writeLong(totalRowsProcessed);
				out.writeLong(totalRowsWritten);
        out.writeLong(totalTime);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				totalRowsProcessed = in.readLong();
				totalRowsWritten = in.readLong();
        totalTime = in.readLong();
    }

    public static class SinkAccumulator{
        private Accumulator processAccumulator;
        private Accumulator sinkAccumulator;
        private long startTime;
        private long finishTime;
        
        public SinkAccumulator(Accumulator processAccumulator,Accumulator sinkAccumulator){
            this.processAccumulator = processAccumulator;
            this.sinkAccumulator = sinkAccumulator;
        }

        public Accumulator readAccumulator(){
            return processAccumulator;
        }

        public Accumulator writeAccumulator(){
            return sinkAccumulator;
        }

        public void start(){
            startTime = System.nanoTime();
            processAccumulator.start();
            sinkAccumulator.start();
        }

        public TaskStats finish(){
            Stats processStats = processAccumulator.finish();
            Stats sinkStats = sinkAccumulator.finish();
            finishTime = System.nanoTime()-startTime;
            return new TaskStats(processStats,sinkStats,finishTime);
        }
        
        public long getStartTime() {
        	return startTime;
        }
        
        public long getFinishTime() {
        	return finishTime;
        }
    }
}

