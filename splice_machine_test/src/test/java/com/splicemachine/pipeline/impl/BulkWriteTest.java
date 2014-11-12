package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.utils.PipelineUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Date: 12/10/13
 */
public class BulkWriteTest {

		@Test
		public void testCanEncodeAndDecodeResultCorrectly() throws Exception {
				BulkWriteResult result = new BulkWriteResult();
				result.setGlobalStatus(new WriteResult(Code.FAILED));
				result.addResult(1, WriteResult.failed("Testing failure"));
				byte[] bytes = PipelineUtils.toCompressedBytes(result);
				BulkWriteResult decoded = PipelineUtils.fromCompressedBytes(bytes,BulkWriteResult.class);
				IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
				Assert.assertNotNull("Incorrect failed rows list!", failedRows.get(1));
				WriteResult writeResult = failedRows.get(1);
				Assert.assertEquals("Incorrect write result!","Testing failure", writeResult.getErrorMessage());
				Assert.assertEquals("Incorrect write result!", Code.FAILED,writeResult.getCode());
		}

		@Test
		public void testCanEncodeAndDecodeWriteCorrectly() throws Exception {
				ObjectArrayList<KVPair> list = new ObjectArrayList<KVPair>();
				KVPair kvPair = new KVPair(Encoding.encode("Hello"),new byte[]{}, KVPair.Type.DELETE);
				list.add(kvPair);
				BulkWrite write = new BulkWrite(list,new ActiveWriteTxn(1l,1l),Bytes.toBytes("sdfsdf"),"dsfsdfdsf");
				byte[] bytes = PipelineUtils.toCompressedBytes(write);
				BulkWrite decoded = PipelineUtils.fromCompressedBytes(bytes,BulkWrite.class);
				ObjectArrayList<KVPair> decList = decoded.getMutations();
				KVPair decPair = decList.get(0);
				Assert.assertEquals("Incorrect pair!","Hello",Encoding.decodeString(decPair.getRow()));
		}
}
