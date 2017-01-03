package com.splicemachine.access.impl.data;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.junit.Test;

/**
 * Created by jleach on 1/3/17.
 */
public class UnsafeRecordTest {

    @Test
    public void testRecordCreationAndMutability() {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                0l);

    }

    @Test
    public void testFoo() {

    }


}