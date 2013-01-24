package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.gotometrics.orderly.Order;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StringRowKey;
import com.gotometrics.orderly.StructBuilder;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * Class to speed up the hashing function for Aggregates, Scalar, Sort
 * 
 * @author johnleach
 *
 */
public class Hasher {
	private static Logger LOG = Logger.getLogger(Hasher.class);
	protected StructRowKey structRowKey;
	protected int[] hashKeys;
	protected Object[] values;
	protected boolean[] sortOrder;
	
	public Hasher(DataValueDescriptor[] descriptors, int[] hashKeys, boolean[] sortOrder, DataValueDescriptor prefixString) {
		this(descriptors,hashKeys,sortOrder,prefixString,null,null);
	}
	
	public Hasher(DataValueDescriptor[] descriptors, int[] hashKeys, boolean[] sortOrder, DataValueDescriptor prefixString, DataValueDescriptor[] additional, boolean[] additionalsortOrder) {
		super();
		try {
		SpliceLogUtils.trace(LOG, "instantiating the struct builder");
		SpliceLogUtils.trace(LOG, "instantiating the struct builder with additional" + hashKeys  + " : " + additional);
		this.hashKeys = hashKeys;
		this.sortOrder = sortOrder;
		if (additional == null)
			values = new Object[hashKeys.length+2];
		else
			values = new Object[hashKeys.length+additional.length+2];
		values[0] = prefixString.getObject();
		StructBuilder structBuilder = new StructBuilder();
		structBuilder.add(new StringRowKey());
		RowKey rowKey;
		for (int i=0;i<hashKeys.length;i++) {
			rowKey = DerbyBytesUtil.getRowKey(descriptors[hashKeys[i]]);
			if (sortOrder != null && !sortOrder[hashKeys[i]])
				rowKey.setOrder(Order.DESCENDING);
			structBuilder.add(rowKey);
		}
        if(additional!=null){
			for (int i=0;i<additional.length;i++) {
				rowKey = DerbyBytesUtil.getRowKey(additional[i]);
				if (additionalsortOrder != null && !additionalsortOrder[i])
					rowKey.setOrder(Order.DESCENDING);
				structBuilder.add(rowKey);
			}
        }
		structBuilder.add(new VariableLengthByteArrayRowKey());	
		structRowKey = structBuilder.toRowKey();
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}
	}
	
	public byte[] generateSortedHashKey(DataValueDescriptor[] descriptors, DataValueDescriptor[] additionalDescriptors) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "generateSortedHashKey");
		int i = 0;
		for (int k = 0;k<hashKeys.length;k++) {
			values[i+1] = descriptors[hashKeys[k]].getObject();
			i++;
		}
		for (int j = 0;j<additionalDescriptors.length;j++) {
			values[i+1] = additionalDescriptors[j].getObject();
			i++;
		}
		values[hashKeys.length+additionalDescriptors.length+1] = SpliceUtils.getUniqueKey();
		return structRowKey.serialize(values);
	}	
	
	public byte[] generateSortedHashKey(DataValueDescriptor[] descriptors) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "generateSortedHashKey");
		for (int i=0;i<hashKeys.length;i++) {
			values[i+1] = descriptors[hashKeys[i]].getObject();
		}
		values[hashKeys.length+1] = SpliceUtils.getUniqueKey();
		return structRowKey.serialize(values);
	}	

	
	/**
	 * 
	 * This method allows for key checking removing the postfix...
	 * 
	 * @param descriptors
	 * @return
	 * @throws StandardException
	 * @throws IOException
	 */
	public byte[] generateSortedHashScanKey(DataValueDescriptor[] descriptors) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "generateSortedHashScanKey");
		for (int i=0;i<hashKeys.length;i++) {
			values[i+1] = descriptors[hashKeys[i]].getObject();
		}
		values[hashKeys.length+1] = null;
		return structRowKey.serialize(values);
	}
	
	public int compareHashKeys(DataValueDescriptor[] left, DataValueDescriptor[] right) throws StandardException, IOException{
		//TODO -sf- can we do this without re-serializing?
		//answer - jl- not that I know of...
		byte[] leftBytes = generateSortedHashScanKey(left);
		byte[] rightBytes = generateSortedHashScanKey(right);
		return Bytes.compareTo(leftBytes,rightBytes);
	}
	
	/**
	 * The purpose of this method is to create the same key for duplicate records thus eliminate the duplicates. 
	 * May need to watch the key collision for different data
	 * @param descriptors
	 * @return
	 * @throws StandardException
	 * @throws IOException
	 */
	public byte[] generateSortedHashKeyWithPostfix(DataValueDescriptor[] descriptors, byte[] postfix) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "generateSortedHashKeyWithPostfix");
		for (int i=0;i<hashKeys.length;i++) {
			values[i+1] = descriptors[hashKeys[i]].getObject();
		}
		values[hashKeys.length+1] = postfix;
		return structRowKey.serialize(values);
	}	
	
}
