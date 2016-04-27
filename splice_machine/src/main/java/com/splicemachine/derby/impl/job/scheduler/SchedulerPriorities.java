package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.RollForwardTask;
import com.splicemachine.derby.impl.job.altertable.AlterTableTask;
import com.splicemachine.derby.impl.job.altertable.PopulateConglomerateTask;
import com.splicemachine.derby.impl.job.index.CreateIndexTask;
import com.splicemachine.derby.impl.job.index.PopulateIndexTask;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.hbase.backup.CreateBackupTask;
import com.splicemachine.hbase.backup.RestoreBackupTask;
import org.apache.hadoop.conf.Configuration;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 12/4/13
 */
public class SchedulerPriorities{
    public static final SchedulerPriorities INSTANCE=new SchedulerPriorities(SpliceConstants.config);
    private static final String TIER_STRATEGY="splice.task.tierStrategy";

    private final Configuration config;
    private final ConcurrentMap<Class<?>, Integer> basePriorityMap;
    private final AtomicInteger systemPriority=new AtomicInteger(0);
    private final double exponentialPriorityScaleFactor;


    private final int numTiers;
    private final int maxPriority;

    private SchedulerPriorities(Configuration config){
        this.config=config;
        this.basePriorityMap=new NonBlockingHashMap<>();
        this.numTiers=config.getInt(SpliceConstants.NUM_PRIORITY_TIERS,SpliceConstants.DEFAULT_NUM_PRIORITY_TIERS);
        this.maxPriority=config.getInt(SpliceConstants.MAX_PRIORITY,SpliceConstants.DEFAULT_MAX_PRIORITY);
        this.exponentialPriorityScaleFactor= (maxPriority-1)/Math.log(1000000000000d);
        setupPriorities();
    }


    public TieredTaskSchedulerSetup getSchedulerSetup(){
        int numThreads=SIConstants.taskWorkers;
        String type=config.get(TIER_STRATEGY);
        if(type==null)
            return SchedulerSetups.uniformSetup(numThreads,numTiers,maxPriority);
        else if(type.equals("binaryNormalized")){
            return SchedulerSetups.binaryNormalizedSetup(numThreads,numTiers,maxPriority);
        }else if(type.equals("uniform")){
            return SchedulerSetups.uniformSetup(numThreads,numTiers,maxPriority);
        }else{
            try{
                @SuppressWarnings("unchecked") Class<? extends TieredTaskSchedulerSetup> setupClass=(Class<? extends TieredTaskSchedulerSetup>)Class.forName(type);
                return setupClass.newInstance();
            }catch(ClassNotFoundException|IllegalAccessException|InstantiationException e){
                throw new RuntimeException(e);
            }
        }
    }

    public int getBaseSystemPriority(){
        return systemPriority.get();
    }

    public int getBasePriority(Class<?> clazz){
        Integer priority=basePriorityMap.get(clazz);
        if(priority==null)
            return maxPriority/2; //default priority is in the middle
        return priority;
    }

    public double getPriorityScaleFactor(){
        return exponentialPriorityScaleFactor;
    }

    private void setupPriorities(){
        /*
		 * Setup default priorities according to configuration and/or reasonable defaults.
		 *
		 * The defaults chosen assume that there are 4 tiers; if there are more than four tiers,
		 * make sure and override as appropriate, lest this not reflect your actual reality
		 */
        String BASE_PRIORITY_PREFIX="splice.task.priority.";
        int baseDefaultPriority=maxPriority/4;
        int defaultSystemPriority=config.getInt(BASE_PRIORITY_PREFIX+"system.default",0);
        systemPriority.set(defaultSystemPriority);

        int defaultDmlReadPriority=config.getInt(BASE_PRIORITY_PREFIX+"dmlRead.default",baseDefaultPriority);
        int defaultDmlWritePriority=config.getInt(BASE_PRIORITY_PREFIX+"dmlWrite.default",2*baseDefaultPriority);
        int defaultDdlWritePriority=config.getInt(BASE_PRIORITY_PREFIX+"ddl.default",3*baseDefaultPriority);
        int defaultBackupPriority=config.getInt(BASE_PRIORITY_PREFIX+"backup.default",4*baseDefaultPriority);

		/*
		 * Manual Registry of default values as defined by me(Scott Fines).
		 *
		 * The essential idea is this:
		 *
		 * 1. System queries are highest priority (ensure that they will almost always have resources available)
		 * 2. DML Read operations next.
		 * 3. DML Write operations
		 * 4. DDL operations
		 *
		 * TODO -sf- adjust this when maintenance tasks are introduced
		 */

        //register DML read operations
        int priority;
        priority=config.getInt(BASE_PRIORITY_PREFIX+"mergeSortJoin",defaultDmlReadPriority);
        basePriorityMap.put(MergeSortJoinOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"groupedAggregate",defaultDmlReadPriority);
        basePriorityMap.put(GroupedAggregateOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"groupedAggregate.distinct",defaultDmlReadPriority);
        basePriorityMap.put(DistinctGroupedAggregateOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"scalarAggregate",defaultDmlReadPriority);
        basePriorityMap.put(ScalarAggregateOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"scalarAggregate.distinct",defaultDmlReadPriority);
        basePriorityMap.put(DistinctScalarAggregateOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"sort",defaultDmlReadPriority);
        basePriorityMap.put(SortOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"scan.distinct",defaultDmlReadPriority);
        basePriorityMap.put(DistinctScalarAggregateOperation.class,priority);

        //DML write operations
        priority=config.getInt(BASE_PRIORITY_PREFIX+"insert",defaultDmlWritePriority);
        basePriorityMap.put(InsertOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"update",defaultDmlWritePriority);
        basePriorityMap.put(UpdateOperation.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"delete",defaultDmlWritePriority);
        basePriorityMap.put(DeleteOperation.class,priority);
        //TODO -sf- add DeleteCascade

        //DDL operations
        priority=config.getInt(BASE_PRIORITY_PREFIX+"import",defaultDdlWritePriority);
        basePriorityMap.put(ImportTask.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"index.create",defaultDdlWritePriority);
        basePriorityMap.put(CreateIndexTask.class,priority);
        basePriorityMap.put(PopulateIndexTask.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"alter.table",defaultDdlWritePriority);
        basePriorityMap.put(AlterTableTask.class,priority);
        basePriorityMap.put(PopulateConglomerateTask.class,priority);

        //maintenance operations
        priority=config.getInt(BASE_PRIORITY_PREFIX+"maintenance",maxPriority);
        basePriorityMap.put(RollForwardTask.class,priority);

        //Backup operations
        priority=config.getInt(BASE_PRIORITY_PREFIX+"backup",defaultBackupPriority);
        basePriorityMap.put(CreateBackupTask.class,priority);
        priority=config.getInt(BASE_PRIORITY_PREFIX+"restore",defaultBackupPriority);
        basePriorityMap.put(RestoreBackupTask.class,priority);
    }

    public int getMaxPriority(){
        return maxPriority;
    }

    public static void main(String... args) throws Exception{
        SchedulerPriorities.INSTANCE.getSchedulerSetup();
    }
}
