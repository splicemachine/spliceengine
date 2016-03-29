package com.splicemachine.hbase.client;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 3/28/16
 */
public class ScanAdjustingResultScannerTest{

    @Test
    public void testWorksInGoodCase() throws Exception{
        final List<Result> results = generateTestData();
        Collections.sort(results,new ResultComparator());

        Scan origScan = new Scan(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW);
        HTableInterface t = mock(HTableInterface.class);
        final AtomicBoolean called = new AtomicBoolean(false);
        when(t.getScanner(any(Scan.class))).thenAnswer(new Answer<ResultScanner>(){
            @Override
            public ResultScanner answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Should not have been reopened!",called.getAndSet(true));
                return new IteratorScanner(results.iterator(),-1,null);
            }
        });

        ResultScanner rs = new ScanAdjustingResultScanner(t,origScan);
        List<Result> ret = new ArrayList<>();
        Result r;
        while((r=rs.next())!=null){
            ret.add(r);
        }

        Assert.assertEquals("Incorrect number of results returned!",results.size(),ret.size());
        for(int i=0;i<results.size();i++){
            Assert.assertEquals("Incorrect ordering!",results.get(i),ret.get(i));
        }
    }

    @Test
    public void testCorrectForOutOfOrderScannerCall() throws Exception{
        final List<Result> results = generateTestData();
        Collections.sort(results,new ResultComparator());

        Scan origScan = new Scan(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW);
        HTableInterface t = mock(HTableInterface.class);
        when(t.getScanner(any(Scan.class))).thenAnswer(new Answer<ResultScanner>(){
            @Override
            public ResultScanner answer(InvocationOnMock invocation) throws Throwable{
                IOException tthrow  =new OutOfOrderScannerNextException("oosne");
                final Scan s = (Scan)invocation.getArguments()[0];
                UnmodifiableIterator<Result> data=Iterators.filter(results.iterator(),new Predicate<Result>(){
                    @Override
                    public boolean apply(Result result){
                        return contained(s,result.getRow());
                    }
                });
                return new IteratorScanner(data,results.size()/2,tthrow);
            }
        });

        ResultScanner rs = new ScanAdjustingResultScanner(t,origScan);
        List<Result> ret = new ArrayList<>();
        Result r;
        while((r=rs.next())!=null){
            ret.add(r);
        }

        Assert.assertEquals("Incorrect number of results returned!",results.size(),ret.size());
        for(int i=0;i<results.size();i++){
            Assert.assertEquals("Incorrect ordering!",results.get(i),ret.get(i));
        }
    }

    @Test
    public void testCorrectForOutOfOrderScannerWrappedInRemote() throws Exception{
        final List<Result> results = generateTestData();
        Collections.sort(results,new ResultComparator());

        Scan origScan = new Scan(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW);
        HTableInterface t = mock(HTableInterface.class);
        when(t.getScanner(any(Scan.class))).thenAnswer(new Answer<ResultScanner>(){
            @Override
            public ResultScanner answer(InvocationOnMock invocation) throws Throwable{
                IOException tthrow  =new RemoteException(OutOfOrderScannerNextException.class.getName(),"re-oosne");
                final Scan s = (Scan)invocation.getArguments()[0];
                UnmodifiableIterator<Result> data=Iterators.filter(results.iterator(),new Predicate<Result>(){
                    @Override
                    public boolean apply(Result result){
                        return contained(s,result.getRow());
                    }
                });
                return new IteratorScanner(data,results.size()/2,tthrow);
            }
        });

        ResultScanner rs = new ScanAdjustingResultScanner(t,origScan);
        List<Result> ret = new ArrayList<>();
        Result r;
        while((r=rs.next())!=null){
            ret.add(r);
        }

        Assert.assertEquals("Incorrect number of results returned!",results.size(),ret.size());
        for(int i=0;i<results.size();i++){
            Assert.assertEquals("Incorrect ordering!",results.get(i),ret.get(i));
        }
    }

    @Test
    public void testCorrectForLeaseExpiration() throws Exception{
        final List<Result> results = generateTestData();
        Collections.sort(results,new ResultComparator());

        Scan origScan = new Scan(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW);
        HTableInterface t = mock(HTableInterface.class);
        when(t.getScanner(any(Scan.class))).thenAnswer(new Answer<ResultScanner>(){
            @Override
            public ResultScanner answer(InvocationOnMock invocation) throws Throwable{
                IOException tthrow  =new LeaseExpiredException("lee");
                final Scan s = (Scan)invocation.getArguments()[0];
                UnmodifiableIterator<Result> data=Iterators.filter(results.iterator(),new Predicate<Result>(){
                    @Override
                    public boolean apply(Result result){
                        return contained(s,result.getRow());
                    }
                });
                return new IteratorScanner(data,results.size()/2,tthrow);
            }
        });

        ResultScanner rs = new ScanAdjustingResultScanner(t,origScan);
        List<Result> ret = new ArrayList<>();
        Result r;
        while((r=rs.next())!=null){
            ret.add(r);
        }

        Assert.assertEquals("Incorrect number of results returned!",results.size(),ret.size());
        for(int i=0;i<results.size();i++){
            Assert.assertEquals("Incorrect ordering!",results.get(i),ret.get(i));
        }
    }


    private boolean contained(Scan s,byte[] row){
        byte[] start = s.getStartRow();
        byte[] end = s.getStopRow();
        ByteComparator bc=Bytes.basicByteComparator();
        if(start==null || start.length<=0){
            if(end==null || end.length<=0) return true;
            else return bc.compare(end,row)>0;
        }else if(end==null || end.length<=0){
            return bc.compare(start,row)<=0;
        }else{
            int c = bc.compare(start,row);
            if(c>0) return false;
            c = bc.compare(end,row);
            return c>0;
        }
    }

    private List<Result> generateTestData(){
        String baserow = "scott";
        List<Result> r = new ArrayList<>();
        for(int i=0;i<10;i++){
            byte[] rk = (baserow+i).getBytes();
            Cell c = new KeyValue(rk,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,rk);
            r.add(Result.create(new Cell[]{c}));
        }
        return r;
    }

    private class IteratorScanner implements ResultScanner{
        private final Iterator<Result> results;
        private final IOException ioe;
        private int throwPos;

        private int pos;

        public IteratorScanner(Iterator<Result> results,int throwPos,IOException ioe){
            this.results=results;
            this.throwPos=throwPos;
            this.ioe=ioe;
        }

        @Override
        public Result next() throws IOException{
            if(!results.hasNext()) return null;
            pos++;
            if(pos==throwPos){
                throwPos=-1;
                throw ioe;
            }
            return results.next();
        }

        @Override
        public Result[] next(int nbRows) throws IOException{
            List<Result> r = new ArrayList<>(nbRows);
            for(int i=0;i<nbRows;i++){
                Result n = next();
                if(n!=null){
                    r.add(n);
                }else break;
            }
            return r.toArray(new Result[r.size()]);
        }

        @Override
        public void close(){

        }

        @Override
        public Iterator<Result> iterator(){
            return results;
        }
    }

    private class ResultComparator implements Comparator<Result>{
        @Override
        public int compare(Result o1,Result o2){
            int c = Bytes.basicByteComparator().compare(o1.getRow(),o2.getRow());
            if(c!=0) return c;
            else{
                Cell[] l = o1.rawCells();
                Cell[] r = o2.rawCells();
                return Bytes.basicByteComparator().compare(CellUtil.cloneQualifier(l[0]),
                        CellUtil.cloneQualifier(r[0]));
            }
        }
    }
}