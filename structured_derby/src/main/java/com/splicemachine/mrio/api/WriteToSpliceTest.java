package com.splicemachine.mrio.api;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.ActiveWriteTxn;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.DerbyDMLWriteInfo;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationInformation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.hbase.writer.AsyncBucketingWriter;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.hbase.writer.Writer.WriteConfiguration;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.hbase.writer.RecordingCallBuffer;

import org.apache.hadoop.conf.Configuration;


public class WriteToSpliceTest{
	private SpliceObserverInstructions instructions;
	private int[] pkCols = null;
	private OperationInformation operationInformation;
	private static final Snowflake snowflake = new Snowflake((short)1);
	//private KryoPool kryoPool = SpliceDriver.getKryoPool();
	private KryoPool kryoPool = null;
	private RowLocation[] autoIncrementRowLocationArray = new RowLocation[0];
	public HTable htable;
	public Configuration config = HBaseConfiguration.create();
	
	public WriteToSpliceTest()
	{
		try {
			htable = new HTable(config, "3024");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		HashPrefix prefix;
		DataHash dataHash;
		KeyPostfix postfix = NoOpPostfix.INSTANCE;
		
		if(pkCols==null){
				//prefix = new SaltedPrefix(operationInformation.getUUIDGenerator());
			prefix = new SaltedPrefix(snowflake.newGenerator(100));
				dataHash = NoOpDataHash.INSTANCE;
		}else{
				int[] keyColumns = new int[pkCols.length];
				for(int i=0;i<keyColumns.length;i++){
						keyColumns[i] = pkCols[i] -1;
				}
				prefix = NoOpPrefix.INSTANCE;
				ExecRow row = getExecRowDefinition();
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion("2.0",true).getSerializers(row);
				//dataHash = BareKeyHash.encoder(keyColumns,null, spliceRuntimeContext.getKryoPool(),serializers);
				//dataHash = BareKeyHash.encoder(keyColumns,null, kryoPool,serializers);
				dataHash = BareKeyHash.encoder(keyColumns,null, serializers);
		}

		return new KeyEncoder(prefix,dataHash,postfix);
}
	public ExecRow getExecRowDefinition() throws StandardException {
		/*
		 * Typically, we just call down to our source and then pass that along
		 * unfortunately, with autoincrement columns this can lead to a
		 * StackOverflow, so we can't do that(see DB-1098 for more info)
		 *
		 * Luckily, DML operations are the top of their stack, so we can
		 * just form our exec row from our result description.
		 */
		//ResultDescription description = writeInfo.getResultDescription();
		//ResultColumnDescriptor[] rcd = description.getColumnInfo();
		ResultColumnDescriptor ds;
		DataValueDescriptor[] dvds = new DataValueDescriptor[]{new SQLVarchar("A"), new SQLInteger(24)};
		/*for(int i=0;i<rcd.length;i++){
				dvds[i] = rcd[i].getType().getNull();
		}*/
		ExecRow row = new ValueRow(dvds.length);
		row.setRowArray(dvds);
//		ExecRow row = source.getExecRowDefinition();
		
		return row;
}
	
	public void doInsert()
	{
		DerbyDMLWriteInfo writeInfo = new DerbyDMLWriteInfo();
		InsertOperation operation = new InsertOperation();
		ObjectArrayList<KVPair> pairs = new ObjectArrayList(10);
		for(int i = 0; i< 10; i++)
		{
			pairs.add(new KVPair(Encoding.encode(i),Encoding.encode(i)));
		}
		byte[] regionKey = HConstants.EMPTY_START_ROW;
	
		
	}
	
	private int[] getEncodingColumns(int n) {
        int[] columns = IntArrays.count(n);

        // Skip primary key columns to save space
        if (pkCols != null) {
            for(int pkCol:pkCols) {
                columns[pkCol-1] = -1;
            }
        }
        return columns;
    }
	
	public DataHash getRowHash(ExecRow row) throws StandardException {
		//get all columns that are being set
		
		int[] columns = getEncodingColumns(row.nColumns());
		DescriptorSerializer[] serializers = VersionedSerializers.forVersion("2.0",true).getSerializers(row);
		return new EntryDataHash(columns,null,serializers);
}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		WriteToSpliceTest writeop = new WriteToSpliceTest();
		SQLUtil sqlUtil = SQLUtil.getInstance();
		try {
			KeyEncoder encoder = writeop.getKeyEncoder(null);
			DataValueDescriptor[] data = new DataValueDescriptor[]{new SQLVarchar("A"), new SQLInteger(24)};
			ExecRow row = new ValueRow(data.length);
			row.setRowArray(data);
			byte[] key = encoder.getKey(row);
			
			System.out.println(new String(key));
			DataHash dataHash = writeop.getRowHash(row);
			dataHash.setRow(row);
			KVPair kv = new KVPair();
			
			kv.setKey(key);
			
			byte[] bdata = dataHash.encode();
			kv.setValue(bdata);
			System.out.println(bdata);
			WriteCoordinator wc = WriteCoordinator.create(writeop.config);
        long transactionID = sqlUtil.getTransactionID();
        Txn txn = new ActiveWriteTxn(transactionID,transactionID);
        RecordingCallBuffer callBuffer = wc.writeBuffer(Bytes.toBytes("3024"), txn, 1);
			//callBuffer.add(kv);
			callBuffer.close();
			wc.shutdown();
			
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
