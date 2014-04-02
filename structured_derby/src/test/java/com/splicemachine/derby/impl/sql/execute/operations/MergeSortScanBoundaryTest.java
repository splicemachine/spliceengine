package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Date: 11/13/13
 */
public class MergeSortScanBoundaryTest {

		@Test
		public void testScanBoundaryCorrect() throws Exception {
				Snowflake snowflake = new Snowflake((short)1);

				MultiFieldEncoder encoder = MultiFieldEncoder.create(mock(KryoPool.class),6);
				encoder.setRawBytes(new byte[1]);
				encoder.setRawBytes(snowflake.nextUUIDBytes());
				encoder.mark();

				List<byte[]> keys = Lists.newArrayList();
				//encode some rights
				for(int i=0;i<2;i++){
						encoder.reset();
						encoder.encodeNext(0);
						encoder.setRawBytes(new byte[]{0x00});
						encoder.setRawBytes(snowflake.nextUUIDBytes());
						encoder.setRawBytes(snowflake.nextUUIDBytes());

						byte[] key = encoder.build();
						key[0] = (byte)0xA0;

						keys.add(key);
				}
				Collections.sort(keys,Bytes.BYTES_COMPARATOR);
				byte[] splitPoint = keys.get(keys.size()-1);

				//encode some lefts
				for(int i=0;i<10;i++){
						encoder.reset();
						encoder.encodeNext(0);
						encoder.setRawBytes(new byte[]{0x01});
						encoder.setRawBytes(snowflake.nextUUIDBytes());
						encoder.setRawBytes(snowflake.nextUUIDBytes());

						byte[] key = encoder.build();
						key[0] = (byte)0xA0;

						keys.add(key);
				}

				//encode some rights with the same hash, but a different key element
				for(int i=0;i<2;i++){
						encoder.reset();
						encoder.encodeNext(3);
						encoder.setRawBytes(new byte[]{0x00});
						encoder.setRawBytes(snowflake.nextUUIDBytes());
						encoder.setRawBytes(snowflake.nextUUIDBytes());

						byte[] key = encoder.build();
						key[0] = (byte)0xA0;

						keys.add(key);
				}

				//make sure everyone is sorted
				Collections.sort(keys, Bytes.BYTES_COMPARATOR);

				ExecRow row  = new ValueRow(1);
				row.setRowArray(new DataValueDescriptor[]{new SQLInteger()});

				MergeSortScanBoundary scanBoundary = new MergeSortScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES, 9);
				byte[] start = scanBoundary.getStartKey(new Result(new KeyValue[]{new KeyValue(splitPoint,SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,splitPoint)}));

				System.out.println(BytesUtil.toHex(start));


		}
}
