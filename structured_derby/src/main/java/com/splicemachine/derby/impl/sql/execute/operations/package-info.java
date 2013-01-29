/**
 * 
<p>  
  This package encapsulates the Splice Machine's parallel operations for generating Derby Result Sets.  Here are the
  list of operations to implement...  

	John:
		Done:
			TableScanResultSet
			BulkTableScanResultSet
			NestedLoopJoinResultSet
      		HashJoinResultSet  
			ScrollInsensitiveResultSet 
    		NestedLoopLeftOuterJoinResultSet
    		HashLeftOuterJoinResultSet
			HashScanResultSet
			ProjectRestrictResultSet
		Working On:
			HashScanResultSet (Bugs)			
	Scott:
		Working On:
			IndexRowToBaseRowResultSet
			RowResultSet
			OnceResultSet
			InsertResultSet
			NormalizeResultSet 
		Done:
			ScalarAggregateResultSet
			GroupedAggregateResultSet
			SortResultSet
	Gene:
		Working On:
			RowCountResultSet
	Jessie:
		Done:
			UnionResultSet
			DistinctScalarAggregateResultSet
			DistinctScanResultSet
			DistinctGroupedAggregateResultSet
		Working On:
			MiscResultSet
			NoRowsResultSet
			
			

	Open To Do Items:
	MiscResultSet
	SetTransactionResultSet 
	InsertVTIResultSet
	DeleteVTIResultSet
	DeleteResultSet
	DeleteCascadeResultSet
	UpdateResultSet
	UpdateVTIResultSet
	DeleteCascadeUpdateResultSet 
	CallStatementResultSet
	HashTableResultSet
	AnyResultSet
	VTIResultSet
	MultiProbeTableScanResultSet
	WindowResultSet
	MaterializedResultSet
	CurrentOfResultSet
    SetOpResultSet
	LastIndexKeyResultSet
	getRaDependentTableScanResultSet
	

 @see org.apache.derby.iapi.sql.execute.ResultSetFactory
   
 */

package com.splicemachine.derby.impl.sql.execute.operations;

