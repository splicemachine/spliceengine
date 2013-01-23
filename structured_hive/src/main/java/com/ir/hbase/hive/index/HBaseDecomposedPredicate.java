package com.ir.hbase.hive.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.ir.hbase.hive.HiveHBaseTableInputFormat;
import com.ir.hbase.hive.objectinspector.HBaseStructObjectInspector;

public class HBaseDecomposedPredicate extends DecomposedPredicate {
	
	public HBaseDecomposedPredicate(ExprNodeDesc predicate, ObjectInspector objectInspector, List<String> columnNames) {
		super();
	    HBaseIndexPredicateAnalyzer analyzer = HiveHBaseTableInputFormat.newIndexPredicateAnalyzer(columnNames);
	    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
	    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate, searchConditions);
	    HBaseStructObjectInspector hbaseStructObjectInspector = (HBaseStructObjectInspector) objectInspector;
	    this.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
	    this.residualPredicate = residualPredicate;
	}
	
	public void evaluateFilters() {
		
	}
	
}
