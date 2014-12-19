package com.splicemachine.pipeline.impl;

import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.si.impl.ActiveWriteTxn;

public class BulkWritesTest {
		@Test
		public void testCanSerializeAndDeserializeCorrectly() throws IOException {
				BulkWrites bws1 = new BulkWrites();
				bws1.setTxn(new ActiveWriteTxn(1l,1l));
				BulkWrites bws2 = new BulkWrites();
				bws2.setTxn(new ActiveWriteTxn(1l,1l));
				BulkWrite bw1 = new BulkWrite("chicken");
				BulkWrite bw2 = new BulkWrite("chicken");
				bw1.addWrite(new KVPair(Bytes.toBytes("yo"),Bytes.toBytes("mtv")));
				bw2.addWrite(new KVPair(Bytes.toBytes("yo"),Bytes.toBytes("mtv")));
				bws1.addBulkWrite(bw1);
				bws2.addBulkWrite(bw2);
				byte[] encoded = PipelineUtils.compressWrite(bws2);
				BulkWrites decoded = PipelineUtils.decompressWrite(encoded);
				Assert.assertEquals(bws1, decoded);
		}

}
