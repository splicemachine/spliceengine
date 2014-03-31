package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.MeasuredResultScanner;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.hbase.client.Result;
import java.io.IOException;
import java.util.List;
import static org.mockito.Mockito.*;


/**
 * Created by jyuan on 3/27/14.
 */
public class ReopenableScannerTest {

    private KryoPool kryoPool = mock(KryoPool.class);
    private static final int tableSize = 10;
    private MultiFieldDecoder decoder = MultiFieldDecoder.create(kryoPool);

    @Test
    public void testMeasuredResultScanner() throws IOException{
        TempTable t = mock(TempTable.class);
        when(t.getCurrentSpread()).thenReturn(SpreadBucket.SIXTEEN);


        SpliceRuntimeContext context = new SpliceRuntimeContext(t, kryoPool);
        ResultScanner rs = mock(ResultScanner.class);
        final List<Result> data = initTable();
        final Scan scan = new Scan();
        HTableInterface table = mock(HTableInterface.class);
        when(table.getScanner(scan)).thenReturn(mock(ResultScanner.class));


        final List<Result> result = Lists.newArrayList();
        Answer<Result> answer = new Answer<Result>() {
            private int n = 0;
            private boolean reopened = false;
            @Override
            public Result answer(InvocationOnMock invocation) throws Throwable {
                int start = 0;

                if (n == 3) {
                    n++;
                    reopened = true;
                    throw new LeaseException("lease expired exception");
                }
                if (reopened) {
                    reopened = false;
                    // mockito did not intercept one of next() call, so advance
                    // position myself
                    n=1;
                }

                byte[] key = scan.getStartRow();
                if (key != null && key.length > 0) {
                    decoder.set(key, 0, key.length);
                    start = decoder.decodeNextInt();
                }

                if (start + n >= tableSize) {
                    return null;
                }
                Result r = data.get(start+n);
                n++;
                return r;
            }
        };
        when(rs.next()).thenAnswer(answer);

        MeasuredResultScanner scanner =
                new MeasuredResultScanner(table, scan, rs, context);
        Result r = null;
        int i = 0;
        while ((r = scanner.next()) != null) {
            compareResult(r, i++);
        }
    }

    private void compareResult(Result r, int i) {

        byte[] row = r.getRow();
        decoder.set(row);
        int key = decoder.decodeNextInt();
        Assert.assertEquals(key, i);
    }
    private List<Result> initTable() {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(kryoPool, 1);
        List<Result> l = Lists.newArrayListWithExpectedSize(tableSize);
        for (int i = 0; i < tableSize; ++i) {
            encoder.reset();
            encoder.encodeNext(i);
            byte[] row = encoder.build();
            KeyValue[] kvs = new KeyValue[1];
            KeyValue kv = new KeyValue(row, SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,row);
            kvs[0] = kv;
            l.add(i, new Result(kvs));
        }
        return l;
    }
}
