package com.splicemachine.derby.impl.load;

import com.google.common.collect.Maps;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.jmx.ManagedTransactor;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.sql.Types;
import java.util.Map;
import java.util.NavigableMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Created on: 10/27/13
 */
public class BlockImportJobTest {
    @Test
    public void testGetTasksGivesOneTaskPerBlock() throws Exception {
        mockTransactions();
        //simulates importing a file with just 1 column
        ImportContext context = new ImportContext.Builder()
                .path("/test")
                .destinationTable(1184l)
                .colDelimiter(",")
                .transactionId("1")
                .addColumn(new ColumnContext.Builder().columnNumber(0).nullable(true).columnType(Types.INTEGER).build())
                .build();

        HTable table = mock(HTable.class);
        NavigableMap<HRegionInfo,ServerName> regionMap = Maps.newTreeMap();
        //ten regions scattered over 4 servers
        byte[] startKey = HConstants.EMPTY_START_ROW;
        byte[] endKey;
        for(int i=0;i<9;i++){
            endKey = Encoding.encode(i);
            HRegionInfo regionInfo = new HRegionInfo(Bytes.toBytes("1184"),
                    startKey,endKey);
            ServerName name = new ServerName("192.168.1."+Integer.toString(i%4)+":8181",System.currentTimeMillis());
            regionMap.put(regionInfo,name);
            startKey = endKey;
        }
        endKey = HConstants.EMPTY_END_ROW;
        HRegionInfo regionInfo = new HRegionInfo(Bytes.toBytes("1184"),
                startKey,endKey);
        ServerName name = new ServerName("192.168.1."+Integer.toString(10%4)+":8181",System.currentTimeMillis());
        regionMap.put(regionInfo,name);

        when(table.getRegionLocations()).thenReturn(regionMap);

        FileSystem fs = mock(FileSystem.class);
        when(fs.exists(any(Path.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                Path p = (Path)invocation.getArguments()[0];
                return p.getName().equals("test");
            }
        });

        //100 blocks, scattered over 5 servers. Each block is 128MB in size
        BlockLocation[] blocks = new BlockLocation[100];
        long offset=0l;
        long length = 128*1024*1024l;
        int serverPos=0;
        for(int i=0;i<blocks.length;i++){
            String[] hosts = new String[3];
            String[] names = new String[3];
            for(int n=0;n<3;n++){
                hosts[n] = "192.168.1."+Integer.toString(serverPos%5);
            }
            blocks[i] = new BlockLocation(names,hosts,offset,length,false);
            offset+=length+1;
        }

        FileStatus testStatus = mock(FileStatus.class);
        when(testStatus.getLen()).thenReturn(offset);
        when(fs.getFileStatus(context.getFilePath())).thenReturn(testStatus);
        when(fs.getFileBlockLocations(any(FileStatus.class),any(Long.class),any(Long.class))).thenReturn(blocks);

        BlockImportJob job = new BlockImportJob(table,context,-1l,-1l,fs);

        Map<? extends RegionTask,Pair<byte[],byte[]>> tasks = job.getTasks();
        /*
         * We need to make sure that:
         *
         * 1. each task is assigned to exactly 1 region
         * 2. The start and end keys are the same
         * 3. No start or end key is []
         */
        for(RegionTask task:tasks.keySet()){
            Pair<byte[],byte[]> boundary = tasks.get(task);
            startKey = boundary.getFirst();
            endKey = boundary.getSecond();

            //make sure neither are equal to 0
            Assert.assertFalse(startKey.length==0);
            Assert.assertFalse(endKey.length==0);

            //make sure the start and end keys are the same
            Assert.assertArrayEquals(startKey,endKey);
        }
    }

    @SuppressWarnings("unchecked")
    private void mockTransactions() throws IOException {
        ManagedTransactor mockTransactor = mock(ManagedTransactor.class);
        doNothing().when(mockTransactor).beginTransaction(any(Boolean.class));

        Transactor mockT = mock(Transactor.class);
        when(mockT.transactionIdFromString(any(String.class))).thenAnswer(new Answer<TransactionId>() {
            @Override
            public TransactionId answer(InvocationOnMock invocation) throws Throwable {
                return new TransactionId((String) invocation.getArguments()[0]);
            }
        });
        when(mockTransactor.getTransactor()).thenReturn(mockT);
        when(mockT.beginChildTransaction(any(TransactionId.class),any(Boolean.class))).thenAnswer(new Answer<TransactionId>() {
            @Override
            public TransactionId answer(InvocationOnMock invocation) throws Throwable {
                return (TransactionId) invocation.getArguments()[0];
            }
        });

        HTransactorFactory.setTransactor(mockTransactor);
    }
}


