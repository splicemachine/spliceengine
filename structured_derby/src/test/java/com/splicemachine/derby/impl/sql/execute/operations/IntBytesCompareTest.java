package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.primitives.Ints;
import com.gotometrics.orderly.IntegerRowKey;
import com.gotometrics.orderly.RowKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 5/7/13
 */
public class IntBytesCompareTest {

    @Test
    public void testCanCompareTwoIntsWithRowKey() throws Exception {
        RowKey rowKey = new IntegerRowKey();

        int val1 = 60209230;
        int val2 = 300501840;

        byte[] bytes1 = rowKey.serialize(val1);
        byte[] bytes2 = rowKey.serialize(val2);

        int bytesCompare = Bytes.compareTo(bytes1,bytes2);

        int intCompare = Ints.compare(val1,val2);
        System.out.printf("bytesCompare=%d,intCompare=%d%n",bytesCompare,intCompare);


        byte[] bytesToCheck = new byte[]{-20,8,-69,44,-105,0};
        System.out.println(rowKey.deserialize(bytesToCheck));

        System.out.println(Integer.MAX_VALUE);
    }
}
