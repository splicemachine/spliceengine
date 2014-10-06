package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;

import org.apache.derby.iapi.types.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.execute.ValueRow;


public class SpliceTableRecordReaderImp{

	private HTable htable = null;
	private ExecRow value = null;
	private ResultScanner scanner = null;
	private Scan scan = null;
	private SQLUtil sqlUtil = null;
	private HashMap<List, List> tableStructure = new HashMap<List, List>();
	private HashMap<List, List> pks = new HashMap<List, List>();
	private ArrayList<String> colNames = new ArrayList<String>();
    private ArrayList<Integer>colTypes = new ArrayList<Integer>();
    private ArrayList<String>pkColNames = new ArrayList<String>();
    private ArrayList<Integer>pkColIds = new ArrayList<Integer>();
    private SpliceTableScannerBuilder builder= null;   
    private SpliceTableScanner tableScanner = null;
    private Configuration conf = null;
    private byte[] lastRow = null;
    private ImmutableBytesWritable rowkey = null;
    
    protected SpliceTableRecordReaderImp(Configuration conf){
    	this.conf = conf;
    }
   
	protected void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
    }

	protected void close() {
		try {
			this.tableScanner.close();
			this.scanner.close();
		} catch (StandardException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void restart(byte[] firstRow) throws IOException {
		Scan newscan = new Scan(scan);
		
		newscan.setStartRow(firstRow);
		
		newscan.setMaxVersions();
		newscan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
		if(htable != null)
			try {
				this.scanner = this.htable.getScanner(newscan);
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				throw e1;
			}
		
		try {
			String transaction_id = conf.get(SpliceMRConstants.SPLICE_TRANSACTION_ID);
			//System.out.println("transaction_id:"+transaction_id);
			buildTableScannerBuilder(transaction_id);
			tableScanner = this.builder.build();
			
			//tableScanner.setColumnTypes(colTypes);
			
		} catch (NumberFormatException e) {
			throw new IOException(e);
		} catch (StandardException e) {
			throw new IOException(e);
		}
		
	}

	protected void init() throws IOException {
		restart(scan.getStartRow());
	}
	
	protected void setScan(Scan scan){
		this.scan = scan;
	}
	
	protected ImmutableBytesWritable getCurrentKey()
	{
		return rowkey;
	}
	
	/**
	 * @return ExecRow (represents a row in Splice)
	 * It will keep on searching the next row until it finds a not-NULL row and then return.
	 * @throws StandardException 
	 * 
	 */
    protected ExecRow getCurrentValue() throws IOException, InterruptedException, StandardException{
    	
    	DataValueDescriptor dvds[] = value.getRowArray();
    	boolean invalid = true;
    	for(DataValueDescriptor d : dvds)
    	{
    		if(!d.isNull())
    		{
    			invalid = false;		
    			break;
    		}
    	}
    	if(invalid)
    	{
    		this.nextKeyValue();
    	}
    	//System.out.println("invalid?"+String.valueOf(invalid));
    	if(invalid)
    		value = null;
        return value;
        
    }
    
    protected EntryDecoder getRowEntryDecoder() {
		return new EntryDecoder();
}
    
    /**
     * 
     * @param columnType (choose from java.sql.Types.*)
     * @return typeFormatId
     * @throws StandardException 
     */
    private int getTypeFormatId(int columnType) throws StandardException
    {
    	return DataTypeDescriptor.getBuiltInDataTypeDescriptor(1).getNull().getTypeFormatId();
    }
    
    /**
     * 
     * @param txsId (transactionID, which is get from SQLUtil.getTransactionId())
     * @throws NumberFormatException
     * @throws StandardException
     * 
     * Build SpliceTableScanner, 
     * rowEncodingMap and rowDecodingMap are set within this function 
     * in order to parse primary key correctly
     */
    private void buildTableScannerBuilder(String txsId) throws NumberFormatException, StandardException
    {  	
		int[] rowEncodingMap;
		
		ExecRow row = new ValueRow(colTypes.size());
		rowEncodingMap = IntArrays.count(colTypes.size());
		
		int []rowDecodingMap = null;
		int []keyColumnOrder = new int[pkColIds.size()];
		int []keyDecodingMap = new int[pkColIds.size()];
		
		for (int i = 0 ; i < pkColIds.size(); i++)
		{
			keyColumnOrder[i] = pkColIds.get(i)-1;
			keyDecodingMap[i] = pkColIds.get(i);
		}

		FormatableBitSet accessedKeyCols = new FormatableBitSet(colTypes.size());
		for(int i = 0; i< keyColumnOrder.length; i++)
		{
			if(keyDecodingMap[i] >= 0)
				accessedKeyCols.set(i);
		}
		int[] keyColumnTypes = null;
		int[] keyEncodingMap = null;
		if(keyColumnOrder!=null){
			
			if(keyEncodingMap==null){
				keyEncodingMap = new int[keyColumnOrder.length];
				for(int i=0;i<keyColumnOrder.length;i++){
					keyEncodingMap[i] = keyDecodingMap[keyColumnOrder[i]];
				}
				
			}
			keyColumnTypes = new int[keyColumnOrder.length];
			for(int i=0;i<keyEncodingMap.length;i++){
				if(keyEncodingMap[i] < 0)
					continue;
				keyColumnTypes[i] = getTypeFormatId(colTypes.get(keyEncodingMap[i]));	
			}
			rowEncodingMap = IntArrays.count(colTypes.size());
			for(int pkCol:keyEncodingMap){
				
				rowEncodingMap[pkCol] = -1;
			}
			
			if(rowDecodingMap==null)
				rowDecodingMap = rowEncodingMap;
		}
	
    	builder = new SpliceTableScannerBuilder()
		.scan(scan)
		.scanner(scanner)	
		.metricFactory(Metrics.basicMetricFactory())
		.transactionID(Long.parseLong(txsId))
		.tableVersion("2.0") // should read table version from derby metadata table
		.rowDecodingMap(rowDecodingMap)
		.template(row.getNewNullRow())
		.indexName(null)
		.setHtable(htable)
		.setColumnTypes(colTypes)
    	
    	.keyColumnEncodingOrder(keyColumnOrder)
    	.keyDecodingMap(keyDecodingMap)
    	.keyColumnTypes(keyColumnTypes)	
    	.accessedKeyColumns(accessedKeyCols);
    }
    
	protected boolean nextKeyValue() throws IOException, InterruptedException, StandardException { 
		if(rowkey == null)
			rowkey = new ImmutableBytesWritable();
		if (value == null)
			value = new ValueRow(0);
			try {
				value = tableScanner.next(null);		
			} catch (IOException e) {
				e.printStackTrace();
				if(lastRow == null)
					lastRow = scan.getStartRow();
				restart(lastRow);
				tableScanner.next(null);
				value = tableScanner.next(null);
			} 
		if (value != null && value.getRowArray().length > 0) {
			lastRow = tableScanner.getCurrentRowLocation().getBytes();
			rowkey.set(lastRow);	
			
			return true;
		}
		
		return false;
	}
    
    protected void setHTable(HTable htable) {
		this.htable = htable;
		Configuration conf = htable.getConfiguration();
		//String tableName = conf.get(TableInputFormat.INPUT_TABLE);
		String tableName = conf.get(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME);
		
		if (sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
		tableStructure = sqlUtil.getTableStructure(tableName);
		pks = sqlUtil.getPrimaryKey(tableName);
		
    	Iterator iter = tableStructure.entrySet().iterator();
    	if(iter.hasNext())
    	{
    		Map.Entry kv = (Map.Entry)iter.next();
    		colNames = (ArrayList<String>)kv.getKey();
    		colTypes = (ArrayList<Integer>)kv.getValue();
    	}
	    	
	    Iterator iterpk = pks.entrySet().iterator();
	    
	    if(iterpk.hasNext())
	    {
	    	Map.Entry kv2 = (Map.Entry)iterpk.next();
	    	pkColNames = (ArrayList<String>)kv2.getKey();
	    	pkColIds = (ArrayList<Integer>)kv2.getValue();
	    }
   
	}

	protected float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}
}
