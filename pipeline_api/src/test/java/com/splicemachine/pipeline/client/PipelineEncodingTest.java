//package com.splicemachine.pipeline.client;
//
//import com.splicemachine.kvpair.KVPair;
//import com.splicemachine.primitives.Bytes;
//import com.splicemachine.si.api.txn.txn;
//import com.splicemachine.si.impl.txn.ActiveWriteTxn;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Iterator;
//
//TODO -sf- move this to hbase_pipeline
//public class PipelineEncodingTest {
//
//    @Test
//    public void testCanEncodeAndDecodeASingleBulkWrite() throws Exception {
//        txn txn = new ActiveWriteTxn(1l,1l);
//
//        Collection<BulkWrite> bws = generateBulkWrites(1);
//        BulkWrites toEncode = new BulkWrites(bws,txn);
//
//        byte[] bytes = PipelineEncoding.encode(toEncode);
//        BulkWrites decoded = PipelineEncoding.decode(bytes);
//        assertMatches("Incorrect BulkWrites",toEncode,decoded);
//    }
//
//    @Test
//    public void testCanEncodeAndDecodeMultipleBulkWrites() throws Exception {
//        txn txn = new ActiveWriteTxn(1l,1l);
//
//        Collection<BulkWrite> bws = generateBulkWrites(10);
//        BulkWrites toEncode = new BulkWrites(bws,txn);
//
//        byte[] bytes = PipelineEncoding.encode(toEncode);
//        BulkWrites decoded = PipelineEncoding.decode(bytes);
//        assertMatches("Incorrect BulkWrites",toEncode,decoded);
//    }
//
//    private void assertMatches(String errorMsgPrefix, BulkWrites correct, BulkWrites actual) {
//        Assert.assertEquals(errorMsgPrefix+": transaction ids don't match!",correct.getTxn().getTxnId(),actual.getTxn().getTxnId());
//        Assert.assertEquals(errorMsgPrefix+": transaction write permission doesn't match!",correct.getTxn().allowsWrites(),actual.getTxn().allowsWrites());
//        Collection<BulkWrite> correctBws = correct.getBulkWrites();
//        Collection<BulkWrite> actualBws = actual.getBulkWrites();
//        Assert.assertEquals(errorMsgPrefix+": bulk write size does not match!",correctBws.size(),actualBws.size());
//        Iterator<BulkWrite> correctIter = correctBws.iterator();
//        Iterator<BulkWrite> actualIter = actualBws.iterator();
//        int pos =0;
//        while(correctIter.hasNext()){
//            BulkWrite cbw = correctIter.next();
//            BulkWrite abw = actualIter.next();
//            Assert.assertEquals(errorMsgPrefix+": Incorrect encodedStringName at pos "+ pos,cbw.getEncodedStringName(),abw.getEncodedStringName());
//
//            Collection<KVPair> cKvs = cbw.getMutations();
//            Collection<KVPair> aKvs = abw.getMutations();
//            Assert.assertEquals(errorMsgPrefix+": Incorrect kvPair size at pos "+ pos,cKvs.size(),aKvs.size());
//            Iterator<KVPair> cKvIter = cKvs.iterator();
//            Iterator<KVPair> aKvIter = aKvs.iterator();
//            while(cKvIter.hasNext()){
//                KVPair cKv = cKvIter.next();
//                KVPair aKv = aKvIter.next();
//                Assert.assertEquals(errorMsgPrefix+": KVPair row not correct",cKv.rowKeySlice(),aKv.rowKeySlice());
//                Assert.assertEquals(errorMsgPrefix+": KVPair value not correct",cKv.valueSlice(),aKv.valueSlice());
//                Assert.assertEquals(errorMsgPrefix+": KVPair type not correct",cKv.getType(),aKv.getType());
//            }
//        }
//    }
//
//    private Collection<BulkWrite> generateBulkWrites(int size) {
//        Collection<BulkWrite> bws = new ArrayList<>(size);
//        for(int i=0;i<size;i++){
//            Collection<KVPair> kvPairs = new ArrayList<>(Arrays.asList(
//                new KVPair(Bytes.toBytes(i), Bytes.toBytes(i + 2),KVPair.Type.INSERT),
//                new KVPair(Bytes.toBytes(i+1), Bytes.toBytes(Integer.toString(2*i)),KVPair.Type.DELETE),
//                    new KVPair(Bytes.toBytes(i+2), Bytes.toBytes(i/.2f),KVPair.Type.UPDATE)
//            ));
//            bws.add(new BulkWrite(kvPairs,Integer.toString(i)));
//        }
//        return bws;
//    }
//}