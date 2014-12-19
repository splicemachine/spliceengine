package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.pipeline.api.Code;
import org.junit.Assert;
import org.junit.Test;

public class BulkWritesResultTest {

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        BulkWritesResult result = new BulkWritesResult();
        IntObjectOpenHashMap<WriteResult> failed = IntObjectOpenHashMap.newInstance();
        failed.put(2,WriteResult.notServingRegion());
        failed.put(1, WriteResult.interrupted());
        result.addResult(new BulkWriteResult(WriteResult.partial(), IntArrayList.from(3,4,5),failed));

        byte[] encoded = result.encode();
        BulkWritesResult decoded = BulkWritesResult.decode(encoded);

        ObjectArrayList<BulkWriteResult> decodedResults = decoded.getBulkWriteResults();
        int decodedSize = decodedResults.size();
        ObjectArrayList<BulkWriteResult> correctResults = result.getBulkWriteResults();
        Assert.assertEquals("Incorrect results list size!",correctResults.size(),decodedSize);
        for(int i=0;i<decodedSize;i++){
            BulkWriteResult decodedResult = decodedResults.get(i);
            assertBulkWritesEqual(correctResults.get(i), decodedResult);
        }
    }

    private void assertBulkWritesEqual(BulkWriteResult correct, BulkWriteResult actual){

        Assert.assertEquals(correct.getNotRunRows(), actual.getNotRunRows());
        IntObjectOpenHashMap<WriteResult> actualFailedRows = actual.getFailedRows();
        IntObjectOpenHashMap<WriteResult> correctFailedRows = correct.getFailedRows();
        Assert.assertEquals("Incorrect size!", correctFailedRows.size(), actualFailedRows.size());
        for(IntObjectCursor<WriteResult> cursor:actualFailedRows){
            WriteResult correctResult = correctFailedRows.get(cursor.key);
            Assert.assertNotNull("Unexpected returned write result!",correct);
            Assert.assertEquals("Incorrect returned error code!", correctResult.getCode(),cursor.value.getCode());
            if(correctResult.getCode()== Code.FAILED)
                Assert.assertEquals("Incorrect returned error message!",correctResult.getErrorMessage(),cursor.value.getErrorMessage());
        }
    }
}
