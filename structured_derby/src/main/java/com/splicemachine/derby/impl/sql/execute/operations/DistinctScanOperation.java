package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jessiezhang
 */
public class DistinctScanOperation extends HashScanOperation
{
	private static Logger LOG = Logger.getLogger(DistinctScanOperation.class);
	
	protected Scan reduceScan;
	
	public DistinctScanOperation() {
		super();
	}
	
    public DistinctScanOperation(long conglomId, 
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		int hashKeyItem,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
    {
		super(conglomId, scoci, activation, resultRowAllocator, resultSetNumber,
			  (GeneratedMethod) null, // startKeyGetter
			  0,					  // startSearchOperator
			  (GeneratedMethod) null, // stopKeyGetter
			  0,					  // stopSearchOperator
			  false,				  // sameStartStopPosition
			  (String) null,	  // scanQualifiers
			  null,	  // nextQualifiers
			  DEFAULT_INITIAL_CAPACITY, DEFAULT_LOADFACTOR, DEFAULT_MAX_CAPACITY,
			  hashKeyItem, tableName, userSuppliedOptimizerOverrides, indexName, isConstraint, 
			  false,				  // forUpdate
			  colRefItem, lockMode, tableLocked, isolationLevel,
			  false,
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		
		// Tell super class to eliminate duplicates
		eliminateDuplicates = true;

		try {
			reduceScan = Scans.buildPrefixRangeScan(sequence[0], transactionID);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		recordConstructorTime();
	}
    
    @Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template){
		SpliceUtils.setInstructions(reduceScan,getActivation(),top);
		return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}
    
    @Override
	public NoPutResultSet executeScan() {
		SpliceLogUtils.trace(LOG,"executeScan");
		beginTime = getCurrentTimeMillis();
		final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(opStack);
		SpliceOperation regionOperation = opStack.get(opStack.size()-1); 
		SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE))
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
		else 
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
		SpliceNoPutResultSet rs = new SpliceNoPutResultSet(activation,this,provider);
		nextTime += getCurrentTimeMillis() - beginTime;
		return rs;
	}
}
