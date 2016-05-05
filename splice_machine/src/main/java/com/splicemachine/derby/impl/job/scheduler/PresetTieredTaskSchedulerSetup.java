package com.splicemachine.derby.impl.job.scheduler;

/**
 * @author Scott Fines
 *         Date: 12/6/13
 */
public class PresetTieredTaskSchedulerSetup implements TieredTaskSchedulerSetup{
    private final int[] priorityLevels;
    private final int[] threads;

    private final long pollTimeMs;
    private final int maintenanceThreads;

    public PresetTieredTaskSchedulerSetup(int[] priorityLevels,int[] threads,int maintenanceThreads){
        this(priorityLevels,threads,maintenanceThreads,200l);
    }

    public PresetTieredTaskSchedulerSetup(int[] priorityLevels,int[] threads,int maintenanceThreads,long pollTimeMs){
        this.priorityLevels=priorityLevels;
        this.threads=threads;
        this.pollTimeMs=pollTimeMs;
        this.maintenanceThreads = maintenanceThreads;
    }

    @Override
    public int[] getPriorityTiers(){
        return priorityLevels;
    }

    @Override
    public int maxThreadsForPriority(int minPriorityForTier){
        int pos=-1;
        for(int priorityLevel : priorityLevels){
            if(priorityLevel>minPriorityForTier)
                break;
            else
                pos++;
        }
        return threads[pos];
    }

    @Override
    public long pollTimeForPriority(int minPriority){
        return pollTimeMs;
    }

    @Override
    public int maxMaintenanceThreads(){
        return maintenanceThreads;
    }
}
