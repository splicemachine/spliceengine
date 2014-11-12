package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.splicemachine.coprocessor.SpliceMessage;

public class AllocatedFilter extends BaseAllocatedFilter<Cell> {

    public AllocatedFilter() {
        super();
    }

    public AllocatedFilter(byte[] localAddress) {
        super(localAddress);
    }
    
	@Override
	public ReturnCode filterKeyValue(Cell ignored) {
		return this.internalFilter(ignored);
	}
	
	   /**
     * @return The filter serialized using pb
     */
    public byte [] toByteArray() {
        SpliceMessage.AllocateFilterMessage.Builder builder =
                SpliceMessage.AllocateFilterMessage.newBuilder();
        if (this.addressMatch != null) builder.setAddressMatch(SpliceZeroCopyByteString.wrap(this.addressMatch));
        return builder.build().toByteArray();
    }

    /**
     * @param addressMatch A pb serialized {@link AllocatedFilter} instance
     * @return An instance of {@link AllocatedFilter} made from <code>bytes</code>
     * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
     * @see #toByteArray
     */
    public static AllocatedFilter parseFrom(final byte [] addressMatch)
            throws DeserializationException {
        SpliceMessage.AllocateFilterMessage proto;
        try {
            proto = SpliceMessage.AllocateFilterMessage.parseFrom(addressMatch);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new AllocatedFilter(proto.hasAddressMatch()?proto.getAddressMatch().toByteArray():null);
    }
}