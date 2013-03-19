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
public class SinkStats implements Externalizable{
    private static final long serialVersionUID = 1l;
    private Stats processStats;
    private Stats sinkStats;
    
    private long totalTime;

    public SinkStats(){}
    
    public Stats getProcessStats(){return processStats;}

    public Stats getSinkStats(){return sinkStats;}

    public long getTotalTime(){ return totalTime;}

    public SinkStats(Stats processStats,Stats sinkStats,long totalTime){
        this.processStats = processStats;
        this.sinkStats = sinkStats;
        this.totalTime = totalTime;
    }

    public static SinkAccumulator uniformAccumulator(){
        return new SinkAccumulator(TimingStats.uniformAccumulator(), TimingStats.uniformAccumulator());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(processStats);
        out.writeObject(sinkStats);
        out.writeLong(totalTime);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        processStats = (Stats)in.readObject();
        sinkStats = (Stats)in.readObject();
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

        public Accumulator processAccumulator(){
            return processAccumulator;
        }

        public Accumulator sinkAccumulator(){
            return sinkAccumulator;
        }

        public void start(){
            startTime = System.nanoTime();
            processAccumulator.start();
            sinkAccumulator.start();
        }

        public SinkStats finish(){
            Stats processStats = processAccumulator.finish();
            Stats sinkStats = sinkAccumulator.finish();
            finishTime = System.nanoTime()-startTime;
            return new SinkStats(processStats,sinkStats,finishTime);
        }
        
        public long getStartTime() {
        	return startTime;
        }
        
        public long getFinishTime() {
        	return finishTime;
        }
    }
}

