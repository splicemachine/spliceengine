package com.splicemachine.derby.impl.job.operation;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.splicemachine.coprocessor.SpliceMessage;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * @author Scott Fines
 * Created on: 5/24/13
 */
public class SuccessFilter extends BaseSuccessFilter<Cell> {
    public SuccessFilter() {
        super();
    }

    public SuccessFilter(List<byte[]> failedTasks) {
    	super(failedTasks);
    }

    /**
     * 
     * Used to filter row key.  Focuses on not forcing a reseek.
     * 
     */
    @Override
	public ReturnCode filterKeyValue(Cell keyValue) {
    	return internalFilter(keyValue);
	}
    @Override
	public byte[] toByteArray() throws IOException {
			SpliceMessage.SuccessFilterMessage filterM = SpliceMessage.SuccessFilterMessage.newBuilder()
							.addAllFailedTasks(Lists.transform(failedTasks,new Function<byte[], ByteString>() {
									@Nullable
									@Override
									public ByteString apply(@Nullable byte[] bytes) {
											return ByteString.copyFrom(bytes);
									}
							})).build();
			return filterM.toByteArray();
	}
	/*
	 * DO NOT DELETE! Used by the ProtobufUtils to deserialize this, and we will break if we don't
	 * implement this.
	 */
	@SuppressWarnings("UnusedDeclaration")
	public static SuccessFilter parseFrom(final byte[] pbBytes) throws DeserializationException{
			SpliceMessage.SuccessFilterMessage proto;
			try{
					proto = SpliceMessage.SuccessFilterMessage.parseFrom(pbBytes);
			}catch(InvalidProtocolBufferException e){
					throw new DeserializationException(e);
			}
			List<byte[]> failedTasks = Lists.transform(proto.getFailedTasksList(),new Function<ByteString, byte[]>() {
					@Nullable
					@Override
					public byte[] apply(@Nullable ByteString bytes) {
							return bytes.toByteArray();
					}
			});
			return new SuccessFilter(failedTasks);
	}
}
