package com.splicemachine.hbase.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Created on: 10/10/13
 */
public class SpliceHTableFactoryTest {

    @Test
    public void testReturnedHTableProperlyExecutesCoprocessorExec() throws Throwable {

        HConnection mockConnection = mock(HConnection.class);

        ExecutorService executor = mock(ExecutorService.class);

//        SpliceHTableFactory factory = new SpliceHTableFactory(false,executor,mockConnection);
//
//        HTableInterface table =  factory.createHTableInterface(new Configuration(),new byte[]{});
//
//        table.coprocessorExec(TestProtocol.class, HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,new Batch.Call<TestProtocol, Void>() {
//            @Override
//            public Void call(TestProtocol instance) throws IOException {
//                instance.test();;
//                return null;
//            }
//        });

    }

    private static interface TestProtocol extends CoprocessorProtocol {

        void test();
    }
}
