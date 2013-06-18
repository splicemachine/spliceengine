package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
/**
 * Class to speed up the hashing function for Aggregates, Scalar, Sort
 * 
 * @author johnleach
 *
 */
public class Hasher {
	private static Logger LOG = Logger.getLogger(Hasher.class);
    protected MultiFieldEncoder encoder;
	protected int[] hashKeys;
	protected boolean[] sortOrder;
    protected boolean[] additionalSortOrder;

    public Hasher(DataValueDescriptor[] descriptors, int[] hashKeys, boolean[] sortOrder, DataValueDescriptor prefixString) {
		this(descriptors,hashKeys,sortOrder,prefixString,null,null);
	}
	/**
	 * 
	 * This is the hasher implementation utilized by the broadcastJoin
	 * 
	 * @param descriptors
	 * @param hashKeys
	 * @param sortOrder
	 */
	public Hasher(DataValueDescriptor[] descriptors, int[] hashKeys, boolean[] sortOrder) {
		super();
        SpliceLogUtils.trace(LOG, "Building hasher with descriptors %s,hashKeys %s",
        		Arrays.toString(descriptors),Arrays.toString(hashKeys));
        this.hashKeys = hashKeys;
        this.sortOrder = sortOrder;
        encoder = MultiFieldEncoder.create(descriptors.length+1);

//        StructBuilder structBuilder = new StructBuilder();
//        structBuilder.add(new StringRowKey());
//        RowKey rowKey;
//        for (int i=0;i<hashKeys.length;i++) {
//        	rowKey = DerbyBytesUtil.getRowKey(descriptors[hashKeys[i]]);
//            if (sortOrder != null && !sortOrder[hashKeys[i]])
//            	rowKey.setOrder(Order.DESCENDING);
//            structBuilder.add(rowKey);
//        }
//        structRowKey = structBuilder.toRowKey();
    }

	
	/**
	 * 
	 * Hasher Implementation for the temp table.
	 * 
	 * @param descriptors
	 * @param hashKeys
	 * @param sortOrder
	 * @param prefixString
	 * @param additional
	 * @param additionalsortOrder
	 */
	public Hasher(DataValueDescriptor[] descriptors, int[] hashKeys, boolean[] sortOrder, DataValueDescriptor prefixString, DataValueDescriptor[] additional, boolean[] additionalsortOrder) {
		super();
        try {
            SpliceLogUtils.trace(LOG, "Building hasher with descriptors %s,hashKeys %s, prefix %s, and additional columns %s",
                    Arrays.toString(descriptors),Arrays.toString(hashKeys),prefixString,Arrays.toString(additional));
            this.hashKeys = hashKeys;
            this.sortOrder = sortOrder;
            this.additionalSortOrder = additionalsortOrder;
            if (additional == null)
                encoder = MultiFieldEncoder.create(hashKeys.length+2);
            else
                encoder = MultiFieldEncoder.create(hashKeys.length+additional.length+2);

            encoder.encodeNext(prefixString.getString());
            encoder.mark();

//            StructBuilder structBuilder = new StructBuilder();
//            structBuilder.add(new StringRowKey());
//            RowKey rowKey;
//            for (int i=0;i<hashKeys.length;i++) {
//                rowKey = DerbyBytesUtil.getRowKey(descriptors[hashKeys[i]]);
//                if (sortOrder != null && !sortOrder[hashKeys[i]])
//                    rowKey.setOrder(Order.DESCENDING);
//                structBuilder.add(rowKey);
//            }
//            if(additional!=null){
//                for (int i=0;i<additional.length;i++) {
//                    rowKey = DerbyBytesUtil.getRowKey(additional[i]);
//                    if (additionalsortOrder != null && !additionalsortOrder[i])
//                        rowKey.setOrder(Order.DESCENDING);
//                    structBuilder.add(rowKey);
//                }
//            }
//            structBuilder.add(new VariableLengthByteArrayRowKey());
//            structRowKey = structBuilder.toRowKey();
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
    }


    private void encodeAll(DataValueDescriptor[] descriptors, DataValueDescriptor[] additionalDescriptors) throws StandardException, IOException {
        encoder.reset();
        for (int k = 0;k<hashKeys.length;k++) {
            encoder = DerbyBytesUtil.encodeInto(encoder, descriptors[hashKeys[k]], sortOrder != null && !sortOrder[hashKeys[k]]);
        }
        for (int j = 0;j<additionalDescriptors.length;j++) {
            encoder = DerbyBytesUtil.encodeInto(encoder,additionalDescriptors[j],additionalSortOrder!=null&&!additionalSortOrder[j]);
        }
    }

    public byte[] generateSortedHashKeyWithoutUniqueKey(DataValueDescriptor[] descriptors, DataValueDescriptor[] additionalDescriptors) throws StandardException, IOException {
        encoder.reset();
		SpliceLogUtils.trace(LOG, "generateSortedHashKey");
        encodeAll(descriptors,additionalDescriptors);
        return encoder.build();
	}

	public byte[] generateSortedHashKey(DataValueDescriptor[] descriptors) throws StandardException, IOException {
        encoder.reset();
		for (int i=0;i<hashKeys.length;i++) {
            encoder = DerbyBytesUtil.encodeInto(encoder, descriptors[hashKeys[i]], sortOrder != null && !sortOrder[hashKeys[i]]);
		}
        encoder = encoder.encodeNextUnsorted(SpliceUtils.getUniqueKey());
        return encoder.build();
	}

	public byte[] generateSortedHashKeyWithoutUniqueKey(DataValueDescriptor[] descriptors) throws StandardException  {
        encoder.reset();
        for (int i=0;i<hashKeys.length;i++) {
            try{
                encoder = DerbyBytesUtil.encodeInto(encoder, descriptors[hashKeys[i]], sortOrder != null && !sortOrder[hashKeys[i]]);
            }catch(IOException ioe){
                throw Exceptions.parseException(ioe);
            }
        }
        return encoder.build();
	}

	/**
	 * 
	 * This method allows for key checking removing the postfix...
	 *
     * @deprecated use {@link #generateSortedHashKeyWithoutUniqueKey(org.apache.derby.iapi.types.DataValueDescriptor[])}
     * instead.
	 * @param descriptors
	 * @return
	 * @throws StandardException
	 * @throws IOException
	 */
    @Deprecated
	public byte[] generateSortedHashScanKey(DataValueDescriptor[] descriptors) throws StandardException {
        return generateSortedHashKeyWithoutUniqueKey(descriptors);
	}
	
	/**
	 * The purpose of this method is to create the same key for duplicate records thus eliminate the duplicates. 
	 * May need to watch the key collision for different data
	 * @param descriptors
	 * @return
	 * @throws StandardException
	 * @throws IOException
	 */
	public byte[] generateSortedHashKeyWithPostfix(DataValueDescriptor[] descriptors, byte[] postfix) throws StandardException {
        encoder.reset();
		SpliceLogUtils.trace(LOG, "generateSortedHashKeyWithPostfix");
		for (int i=0;i<hashKeys.length;i++) {
            try{
                encoder = DerbyBytesUtil.encodeInto(encoder,descriptors[hashKeys[i]],sortOrder!=null&&!sortOrder[hashKeys[i]]);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }
        encoder = encoder.encodeNextUnsorted(postfix);
		return encoder.build();
	}

    public byte[] getPrefixBytes() throws IOException {
        return encoder.getEncodedBytes(0);
    }
    
}
