package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import java.util.HashSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.Sets;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractBufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;

public class GroupedAggregateBufferedAggregator extends AbstractBufferedAggregator {
    private static final Logger LOG = Logger.getLogger(GroupedAggregateBufferedAggregator.class);
    private final boolean eliminateDuplicates;
    private final boolean shouldMerge;
    private IntObjectMap<HashSet<DataValueDescriptor>> uniqueValues;
		private final boolean shouldFinish;

    protected GroupedAggregateBufferedAggregator(SpliceGenericAggregator[] aggregates,
                                 boolean eliminateDuplicates,
                                 boolean shouldMerge,
                                 StandardSupplier<ExecRow> emptyRowSupplier,
                                 WarningCollector warningCollector,
                                 boolean shouldFinish) {
    	super(aggregates,emptyRowSupplier,warningCollector);
        this.eliminateDuplicates = eliminateDuplicates;
        this.shouldMerge = shouldMerge;
        this.shouldFinish = shouldFinish;
    }
    
    public void initialize(ExecRow row) throws StandardException{
        this.currentRow = row.getClone();
				for(SpliceGenericAggregator aggregator:aggregates){
						initializeAggregate(aggregator,currentRow);
				}
    }

		private void initializeAggregate(SpliceGenericAggregator aggregator, ExecRow currentRow) throws StandardException {
				if (!filterDistincts(currentRow,aggregator,true)) {
						if(aggregator.initialize(currentRow))
								aggregator.accumulate(currentRow,currentRow);
				}
		}

    public void merge(ExecRow newRow) throws StandardException{
				/*
				 * In previous incarnations, we needed to initialize aggregates for every row that came in to play. This
				 * was because Derby assumed that every row had an Aggregator object in the proper location.
				 *
				 * In order to remove this, we allow aggregators to add null entries (or, more appropriately, ignore null
				 * entries), which allows us to avoid initialization of every row.
				 */
				for(SpliceGenericAggregator aggregator:aggregates) {
						if (shouldMerge){
								if(!filterDistincts(newRow,aggregator,true))
										aggregator.merge(newRow,currentRow);
						}else if(!filterDistincts(newRow,aggregator,true))
								aggregator.accumulate(newRow,currentRow);
				}
		}


    public boolean filterDistincts(ExecRow newRow,
                                    SpliceGenericAggregator aggregator,
                                    boolean addEntry) throws StandardException {
        if(aggregator.isDistinct()){
            if(uniqueValues==null)
                uniqueValues = IntObjectOpenHashMap.newInstance();

            int inputColNum = aggregator.getAggregatorInfo().getInputColNum();
            HashSet<DataValueDescriptor> uniqueVals = uniqueValues.get(inputColNum);
            if(uniqueVals==null){
                uniqueVals = Sets.newHashSet();
                uniqueValues.put(inputColNum,uniqueVals);
            }

            DataValueDescriptor uniqueValue = aggregator.getInputColumnValue(newRow);
            if(uniqueVals.contains(uniqueValue)){
            	if(LOG.isTraceEnabled())
                    LOG.trace("Aggregator "+ aggregator+" is removing entry "+ newRow);
                return true;
            }
            if(addEntry) {
                uniqueVals.add(uniqueValue.cloneValue(false));
            }
        }
        return false;
    }

    public boolean isInitialized() {
        return currentRow!=null;
    }

    public ExecRow finish() throws StandardException{
        if(currentRow==null)
            currentRow = emptyRowSupplier.get();

        boolean eliminatedNulls = false;
				if (shouldFinish) {
						for(SpliceGenericAggregator aggregate:aggregates){
//								if (!aggregate.isInitialized(currentRow))
//										initializeAggregate(aggregate,currentRow);
								if(aggregate.finish(currentRow))
										eliminatedNulls=true;
						}
        }
//        if(eliminatedNulls)
//            warningCollector.addWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION);

        //once finished, return this to an unitialized state so it can be reused
        ExecRow toReturn = currentRow;
        currentRow= null;
        if(uniqueValues!=null)
            uniqueValues.clear();
        return toReturn;
    }
}