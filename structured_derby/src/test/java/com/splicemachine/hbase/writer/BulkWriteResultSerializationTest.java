package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 2/3/14
 */
@RunWith(Parameterized.class)
public class BulkWriteResultSerializationTest {

		@Parameterized.Parameters
		public static Collection<Object[]> data() {
				Collection<Object[]> data = Lists.newArrayList();

				for(WriteResult.Code code:WriteResult.Code.values()){
					data.add(new Object[]{code,"testErrorMessage"});
				}
				return data;
		}

		private final WriteResult.Code code;
		private final String errorMessage;

		public BulkWriteResultSerializationTest(WriteResult.Code code, String errorMessage) {
				this.code = code;
				this.errorMessage = errorMessage;
		}

		@Test
		public void testCanSerializeFailedRowsCorrectly() throws Exception {
				IntArrayList notRunRows = IntArrayList.newInstanceWithCapacity(10);
				for(int i=10;i<15;i++){
						notRunRows.add(i);
				}
				IntObjectOpenHashMap<WriteResult> failedRows = IntObjectOpenHashMap.newInstance();
				for(int i=0;i<10;i++){
						failedRows.put(i,new WriteResult(code,errorMessage));
				}
				BulkWriteResult result = new BulkWriteResult(notRunRows,failedRows);

				byte[] data = result.toBytes();
				BulkWriteResult decoded = BulkWriteResult.fromBytes(data);

				Assert.assertEquals(notRunRows, decoded.getNotRunRows());
				IntObjectOpenHashMap<WriteResult> decodedFailedRows = decoded.getFailedRows();
				Assert.assertEquals("Incorrect decoded size!", failedRows.size(), decodedFailedRows.size());
				for(IntObjectCursor<WriteResult> cursor:decodedFailedRows){
						WriteResult correct = failedRows.get(cursor.key);
						Assert.assertNotNull("Unexpected returned write result!",correct);
						Assert.assertEquals("Incorrect returned error code!", cursor.value.getCode(), correct.getCode());
						if(correct.getCode()== WriteResult.Code.FAILED)
								Assert.assertEquals("Incorrect returned error message!",cursor.value.getErrorMessage(),correct.getErrorMessage());
				}
		}
}
