package com.ir.hbase.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.ir.constants.HBaseConstants;
import com.ir.constants.SchemaConstants;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.hive.index.HBaseIndexPredicateAnalyzer;
import com.ir.hbase.index.query.QueryUtils;
import com.ir.hbase.index.query.ScannerKeyElement;

public class HiveHBaseTableInputFormat extends TableInputFormatBase implements InputFormat<ImmutableBytesWritable, Result> {
	static final Log LOG = LogFactory.getLog(org.apache.hadoop.mapreduce.v2.app.MRAppMaster.class);
	private IndexTableStructure is;
	private TableStructure ts;
	private Map<String,IndexSearchCondition> searchConditionMap = new HashMap<String,IndexSearchCondition>();
	private int preferredIndexCount = 0;
	private Index preferredIndex = null;
	private byte[] beginKey = null;
	private byte[] endKey = null;
	public static final String EQUAL = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual";
	public static final String EQUAL_GREATER_THAN = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan";
	public static final String EQUAL_LESSER_THAN = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan";
	public static final String GREATER_THAN = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan";
	public static final String LESSER_THAN = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan";
	public static final String NOT_EQUAL = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual";
	private LinkedHashMap<String, ScannerKeyElement> scanColumnsForBeginKey = new LinkedHashMap<String,ScannerKeyElement>();
	private LinkedHashMap<String, ScannerKeyElement> scanColumnsForEndKey = new LinkedHashMap<String,ScannerKeyElement>();
	private List<String> readColumns = new ArrayList<String>();
 	private FilterList filterList = new FilterList();
	private String PRIMARY_SCAN_TABLE_NAME = "PRIMARY_SCAN_TABLE_NAME";
	private HTable primaryTable = null;



	@Override
	public RecordReader<ImmutableBytesWritable, Result> getRecordReader(InputSplit split,JobConf jobConf,final Reporter reporter) throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("get Record Reader Called ");		
		}
		HBaseSplit hbaseSplit = (HBaseSplit) split;
		TableSplit tableSplit = hbaseSplit.getSplit();
		String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
		is = IndexTableStructure.fromJSon(jobConf.get(HBaseStorageHandler.INDEX_STRUCTURE));
		ts = TableStructure.fromJSON(jobConf.get(HBaseStorageHandler.TABLE_STRUCTURE));
		Scan scan = new Scan();
		scan.setCaching(1000);
		refresh();
		convertFilter2(jobConf);
		String columnList = jobConf.get(Constants.LIST_COLUMNS);
		if (columnList != null) {
			List<String> columnNames = Arrays.asList(columnList.split(","));
			for (int readColID : ColumnProjectionUtils.getReadColumnIDs(jobConf)) {
				try {
					readColumns.add(ts.getColumn(columnNames.get(readColID)).getFullColumnName());
				} catch (NullPointerException e) {
					throw new RuntimeException("Read Column ID:" + readColID + " ColumnName from hive:" + columnNames.get(readColID) + " doesn't macth that from HBase:" + ts.getColumn(columnNames.get(readColID)));
				}
			}
		}
		if (preferredIndex != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Scanning Index Table "+ hbaseTableName + SchemaConstants.INDEX +"with begin key " + beginKey + " and end key " + endKey);
			}	
			WhileMatchFilter filter = new WhileMatchFilter(filterList);
			scan.setFilter(filter);
			setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName + SchemaConstants.INDEX)));
			List<String> indexedColumns = new ArrayList<String>();
			for (Column column : preferredIndex.getAllColumns())
				indexedColumns.add(column.getFullColumnName());
			for (String fullColumnName : readColumns) {
				if (!indexedColumns.contains(fullColumnName)) {
					primaryTable = new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName));
					break;
				}
			}
			if (primaryTable == null) {
				for (String fullColumnName : readColumns) {
					String[] splits = fullColumnName.split(HBaseConstants.FAMILY_SEPARATOR);
					scan.addColumn(splits[0].getBytes(), splits[1].getBytes());
				}
			}
			scan.addColumn(SchemaConstants.INDEX_BASE_FAMILY_BYTE, SchemaConstants.INDEX_BASE_ROW_BYTE);
		} else {
			if (LOG.isDebugEnabled())
				LOG.debug("Scanning Base Table");	
			for (String fullColumnName : readColumns) {
				String[] splits = fullColumnName.split(HBaseConstants.FAMILY_SEPARATOR);
				scan.addColumn(splits[0].getBytes(), splits[1].getBytes());
			}
			setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName)));				
		}
		setScan(scan);
		Job job = Job.getInstance(jobConf);
		TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(job.getConfiguration(), reporter);
		final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result> recordReader = createRecordReader(tableSplit, tac);
		return new RecordReader<ImmutableBytesWritable, Result>() {

			@Override
			public void close() throws IOException {
				recordReader.close();
			}

			@Override
			public ImmutableBytesWritable createKey() {
				return new ImmutableBytesWritable();
			}

			@Override
			public Result createValue() {
				return new Result();
			}

			@Override
			public long getPos() throws IOException {
				return 0;
			}

			@Override
			public float getProgress() throws IOException {
				float progress = 0.0F;

				try {
					progress = recordReader.getProgress();
				} catch (InterruptedException e) {
					throw new IOException(e);
				}

				return progress;
			}

			@Override
			public boolean next(ImmutableBytesWritable rowKey, Result value) throws IOException {

				boolean next = false;
				try {
					next = recordReader.nextKeyValue();
					if (next) {
						if (preferredIndex == null) {
							rowKey.set(recordReader.getCurrentValue().getRow());
							Writables.copyWritable(recordReader.getCurrentValue(), value);
						} else if (primaryTable == null) { 
							Result result = recordReader.getCurrentValue();
							rowKey.set(result.getValue(SchemaConstants.INDEX_BASE_FAMILY_BYTE, SchemaConstants.INDEX_BASE_ROW_BYTE));
							Writables.copyWritable(result, value);
						} else {
							Result result = recordReader.getCurrentValue();
							byte[] baseRow = result.getValue(SchemaConstants.INDEX_BASE_FAMILY_BYTE, SchemaConstants.INDEX_BASE_ROW_BYTE);
							rowKey.set(baseRow);
							Get get = new Get(baseRow);
							for (String fullColumnName : readColumns) {
								String[] splits = fullColumnName.split(HBaseConstants.FAMILY_SEPARATOR);
								get.addColumn(splits[0].getBytes(), splits[1].getBytes());
							}
							Result primaryResult = primaryTable.get(get);
							Writables.copyWritable(primaryResult, value);
						}
					}
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
				return next;
			}
		};
	}

	/**
	 * Instantiates a new predicate analyzer suitable for
	 * determining how to push a filter down into the HBase scan,
	 * based on the rules for what kinds of pushdown we currently support.
	 *
	 * @param keyColumnName name of the Hive column mapped to the HBase row key
	 *
	 * @return preconfigured predicate analyzer
	 */
	public static HBaseIndexPredicateAnalyzer newIndexPredicateAnalyzer(List<String> columnNames) {
		HBaseIndexPredicateAnalyzer analyzer = new HBaseIndexPredicateAnalyzer();	
		analyzer.addComparisonOp(EQUAL);
		analyzer.addComparisonOp(EQUAL_GREATER_THAN);
		analyzer.addComparisonOp(EQUAL_LESSER_THAN);
		analyzer.addComparisonOp(GREATER_THAN);
		analyzer.addComparisonOp(LESSER_THAN);
		analyzer.addComparisonOp(NOT_EQUAL);
		analyzer.clearAllowedColumnNames();
		for (String columnName: columnNames) {
			analyzer.allowColumnName(columnName);
		}
		return analyzer;
	}

	private void convertFilter2(JobConf jobConf) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("convertFilter called");
		}
		String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
		if (filterExprSerialized == null) {
			return;
		}
		ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized, jobConf);
		String columnNameProperty = jobConf.get(Constants.LIST_COLUMNS);
		if (LOG.isDebugEnabled()) {
			LOG.debug("convertFilter list Columns " + columnNameProperty);
		}
		List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		HBaseIndexPredicateAnalyzer analyzer = newIndexPredicateAnalyzer(columnNames);
		List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
		ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);
		for (IndexSearchCondition indexSearchCondition : searchConditions) {
			searchConditionMap.put(indexSearchCondition.getColumnDesc().getColumn(), indexSearchCondition);
		}
		/*if (residualPredicate != null) {
      throw new RuntimeException(
        "Unexpected residual predicate " + residualPredicate.getExprString());
    }
		 */
		determineIndexToUse();
		try {
			createFilterListAndKeys();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public void determineIndexToUse() {
		if (LOG.isDebugEnabled())
			LOG.debug("HBaseQuery.determineIndexToUse called");	
		for (Index index : is.getIndexes()) {
			int count = 0;
			for (IndexColumn indexColumn: index.getIndexColumns()) {
				if (searchConditionMap.containsKey(indexColumn.getColumnName())) {
					count++;
					if (searchConditionMap.get(indexColumn.getColumnName()).getComparisonOp().equals(EQUAL))
						continue; // Can Only Do Composite Indexes in the Case of Equality
				}
				break;
			}

			if (LOG.isDebugEnabled())
				LOG.debug("HBaseQuery.determineIndexToUseScore : " + index.getIndexName() + " : " + count);	
			if (count > preferredIndexCount &&  count > 0) {
				preferredIndexCount = count;
				preferredIndex = index;
			}
		}
		if (LOG.isDebugEnabled() && preferredIndex != null)
			LOG.debug("HBaseQuery.determineIndexToUseWinner " + preferredIndex.getIndexName() + " : " + preferredIndexCount);
		if (LOG.isDebugEnabled() && preferredIndex == null)
			LOG.debug("HBaseQuery.determineIndexToUseNoWinner");
	}

	public void createFilterListAndKeys() throws Exception {
		if (LOG.isDebugEnabled())
			LOG.debug("HBaseQuery.createFilterListAndKeys called");
		if (preferredIndex != null) {
			int count = 0;
			boolean descending = false;
			IndexColumn icol;
			for (IndexColumn indexColumn : preferredIndex.getIndexColumns()) {
				if (count < this.preferredIndexCount) {
					count++;
					IndexSearchCondition indexSearchCondition = searchConditionMap.get(indexColumn.getColumnName());
					String op = indexSearchCondition.getComparisonOp();		
					CompareOp compareOp = null;
					descending = indexColumn.getOrder().equals(Order.DESCENDING);
					String fullColumnName = indexColumn.getFullColumnName();
					Column.Type type = HiveHBaseUtil.hiveToHBase(indexSearchCondition.getColumnDesc().getTypeString());
					Object value = indexSearchCondition.getConstantDesc().getValue();
					if (op.equals(EQUAL)) {
						compareOp = CompareOp.EQUAL;
						scanColumnsForBeginKey.put(fullColumnName, new ScannerKeyElement(value));
					} else if (op.equals(NOT_EQUAL)) {
						compareOp = CompareOp.NOT_EQUAL;
					}
					else if (op.equals(GREATER_THAN)) {					
						compareOp = CompareOp.GREATER;
						if (descending) {
							scanColumnsForEndKey.put(fullColumnName, new ScannerKeyElement(value));							
						} else {
							scanColumnsForBeginKey.put(fullColumnName, new ScannerKeyElement(value, true, true));
						}
					}
					else if (op.equals(EQUAL_GREATER_THAN)) {
						compareOp = CompareOp.GREATER_OR_EQUAL;
						if (descending) {
							scanColumnsForEndKey.put(fullColumnName,new ScannerKeyElement(value,true, true));  
						} else {
							scanColumnsForBeginKey.put(fullColumnName,new ScannerKeyElement(value)); //TODO: XXX Check increment correctness DoubleIndexTest testDoubleGreaterThanEqualTo
						}
					}
					else if (op.equals(LESSER_THAN)) {
						compareOp = CompareOp.LESS;
						if (descending) {
							scanColumnsForBeginKey.put(fullColumnName,new ScannerKeyElement(value));
						} else {
							scanColumnsForEndKey.put(fullColumnName,new ScannerKeyElement(value));
						}
					}
					else if (op.equals(EQUAL_LESSER_THAN)) {
						compareOp = CompareOp.LESS_OR_EQUAL;
						if (descending) {
							scanColumnsForBeginKey.put(fullColumnName,new ScannerKeyElement(value, true, true));
						} else {
							scanColumnsForEndKey.put(fullColumnName,new ScannerKeyElement(value, true, true));
						}
					}
					else {

					}
					filterList.addFilter(new com.ir.hbase.index.query.SingleColumnValueFilter(indexColumn.getFamily(),indexColumn.getColumnName(),compareOp,BytesUtil.toBytes(value, type),preferredIndex));						
				}

				beginKey = QueryUtils.generateIndexKeyForQuery(preferredIndex,scanColumnsForBeginKey,false);
				endKey = QueryUtils.generateIndexKeyForQuery(preferredIndex,scanColumnsForEndKey,true);
			}
		}
	}

	/*
public static byte[] generateIndexKeyForQuery(Index index, Map<String,ScannerKeyElement> scanColumns) {
	List<byte[]> fromKeyComponents = new ArrayList<byte[]>();
	fromKeyComponents.add(Bytes.toBytes(index.getIndexName()));
	fromKeyComponents.add(SchemaConstants.INDEX_DELIMITER);
	for (IndexColumn icol : index.getIndexColumns()) {
		Object value = scanColumns.get(icol.getFullColumnName());
		if (value == null)
			break;
		byte[] bytes = Index.dataToBytes(icol, ((ScannerKeyElement) value).getBytes());
		if (((ScannerKeyElement) value).getIncrement()) {
			BytesUtil.incrementAtIndex(bytes, bytes.length - 1);
		}
		fromKeyComponents.add(bytes);
		fromKeyComponents.add(SchemaConstants.INDEX_DELIMITER);			
	}
	return BytesUtil.concat(fromKeyComponents);
}
	 */

	public void refresh() {
		beginKey = null;
		endKey = null;
		preferredIndexCount = 0;
		preferredIndex = null;
		searchConditionMap = new HashMap<String,IndexSearchCondition>();
		scanColumnsForBeginKey = new LinkedHashMap<String,ScannerKeyElement>();
		scanColumnsForEndKey = new LinkedHashMap<String,ScannerKeyElement>();
		filterList = new FilterList();	
	}

	@Override
	public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getSplits called");
		}

		String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
		is = IndexTableStructure.fromJSon(jobConf.get(HBaseStorageHandler.INDEX_STRUCTURE));	
		setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName)));
		refresh();
		convertFilter2(jobConf);
		Scan scan = new Scan();
		scan.setCaching(1000);
		if (preferredIndex != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Scanning Index Table "+ hbaseTableName + SchemaConstants.INDEX +"with begin key " + beginKey + " and end key " + endKey);
			}	
			jobConf.set(PRIMARY_SCAN_TABLE_NAME, hbaseTableName + SchemaConstants.INDEX);
			if (beginKey != null)
				scan.setStartRow(beginKey);
			if (endKey != null)
				scan.setStopRow(endKey);
			System.out.println("Utilizing Index " + preferredIndex.getIndexName() + " using begin key " + beginKey + " and end key " + endKey);
			WhileMatchFilter filter = new WhileMatchFilter(filterList);
			scan.setFilter(filter);
			setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName+SchemaConstants.INDEX)));				
		} else {
			if (LOG.isDebugEnabled())
				LOG.debug("Scanning Base Table");	
			jobConf.set(PRIMARY_SCAN_TABLE_NAME, hbaseTableName);
			setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName)));				
		}
		setScan(scan);
		Job job = Job.getInstance(jobConf);
		JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
		Path [] tablePaths = FileInputFormat.getInputPaths(jobContext);

		List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(jobContext);
		InputSplit [] results = new InputSplit[splits.size()];
		for (int i = 0; i < splits.size(); i++) {
			results[i] = new HBaseSplit((TableSplit) splits.get(i), tablePaths[0]);
		}
		return results;
	}


}