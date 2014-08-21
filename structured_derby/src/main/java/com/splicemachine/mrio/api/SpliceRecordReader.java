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


public class SpliceRecordReader extends SpliceTableRecordReaderBase{

	private HTable htable = null;
	private ImmutableBytesWritable rowkey = null;
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
    
    public SpliceRecordReader(Configuration conf)
    {
    	super();
    	this.conf = conf;
    }
    
	@Override
	public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
       
		System.out.println("Initializing SpliceRecordReader....");
		sqlUtil = SQLUtil.getInstance();
    }
	
	@Override
	public void close() {
		try {
			this.tableScanner.close();
			this.scanner.close();
			
			
		} catch (StandardException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	@Override
	public void restart(byte[] firstRow) {
		scan = new Scan();
		
		scan.setStartRow(firstRow);
		scan.setMaxVersions();
		scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
		if(htable != null)
			try {
				this.scanner = this.htable.getScanner(scan);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		try {
			String transaction_id = conf.get(SpliceMRConstants.SPLICE_TRANSACTION_ID);
			buildTableScannerBuilder(transaction_id);
			tableScanner = this.builder.build();
			//tableScanner.setColumnTypes(colTypes);
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (StandardException e) {
			e.printStackTrace();
		}
		
	}

	public void init() throws IOException {
		restart(scan.getStartRow());
	}
	
	@Override
    public ImmutableBytesWritable getCurrentKey(){
	
        return rowkey;
    }
	
	/**
	 * @return ExecRow (represents a row in Splice)
	 * It will keep on searching the next row until it finds a not-NULL row and then return.
	 * 
	 */
    @Override
    public ExecRow getCurrentValue() throws IOException, InterruptedException{
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
     */
    private int getTypeFormatId(int columnType)
    {
    	switch(columnType)
		{
				case java.sql.Types.INTEGER:
					return new SQLInteger(1).getTypeFormatId();
				case java.sql.Types.BIGINT:
					return new SQLLongint(1).getTypeFormatId();				
				case java.sql.Types.TIMESTAMP:
					return new SQLTimestamp().getTypeFormatId();
				case java.sql.Types.TIME:
					return new SQLTime().getTypeFormatId();					
				case java.sql.Types.SMALLINT:
					return new SQLSmallint().getTypeFormatId();
				case java.sql.Types.BOOLEAN:
					return new SQLBoolean().getTypeFormatId();
				case java.sql.Types.DOUBLE:
					return new SQLDouble().getTypeFormatId();
				case java.sql.Types.FLOAT:
					return new SQLReal().getTypeFormatId();
				case java.sql.Types.CHAR:
					return new SQLChar().getTypeFormatId();
				case java.sql.Types.VARCHAR:
					return new SQLVarchar().getTypeFormatId();
				case java.sql.Types.BINARY:
					return new SQLBlob().getTypeFormatId();
				default:
					return new org.apache.derby.iapi.types.SQLClob().getTypeFormatId();
		}
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
		.transactionID(txsId)
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
    
    @Override
	public boolean nextKeyValue() throws IOException, InterruptedException { 
		if (rowkey == null)
			rowkey = new ImmutableBytesWritable();
		if (value == null)
			value = new ValueRow(0);
			try {
				value = tableScanner.next(null);		
			} catch (StandardException e) {
				throw new IOException(e.getMessage());
			} 
		if (value != null && value.getRowArray().length > 0) {
			/*rowkey.set(value.getRow());
			lastRow = rowkey.get();*/
			return true;
		}
		
		return false;
	}
    
    public void setHTable(HTable htable) {
		this.htable = htable;
		Configuration conf = htable.getConfiguration();
		//String tableName = conf.get(TableInputFormat.INPUT_TABLE);
		String tableName = conf.get(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME);
		
		if (sqlUtil == null)
			sqlUtil = SQLUtil.getInstance();
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
}
