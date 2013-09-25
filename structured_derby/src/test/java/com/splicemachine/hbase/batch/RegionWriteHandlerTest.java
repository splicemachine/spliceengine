package com.splicemachine.hbase.batch;

import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.tools.ResettableCountDownLatch;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.splicemachine.hbase.MockRegion.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 9/25/13
 */
public class RegionWriteHandlerTest {

    @Test
    public void testWritesRowsCorrectly() throws Exception {
        final List<Mutation> successfulPuts = Lists.newArrayList();
        HRegion testRegion = getMockRegion(getSuccessOnlyAnswer(successfulPuts));
        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));

        List<KVPair> pairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            pairs.add(next);
            testContext.sendUpstream(next);
        }

        //make sure that nothing has been written to the region
        Assert.assertEquals("Writes have made it to the region!", 0, successfulPuts.size());

        //finish
        Map<KVPair,WriteResult> finish = testContext.finish();

        //make sure that the finish has the correct (successful) WriteResult
        for(WriteResult result:finish.values()){
            Assert.assertEquals("Incorrect result!",WriteResult.Code.SUCCESS,result.getCode());
        }

        //make sure that all the KVs made it to the region
        Assert.assertEquals("incorrect number of rows made it to the region!",pairs.size(),successfulPuts.size());

        for(Mutation mutation:successfulPuts){
            boolean found = false;
            for(KVPair pair:pairs){
                found = Bytes.equals(mutation.getRow(),pair.getRow());
                if(found)
                    break;
            }
            Assert.assertTrue("Row "+ BytesUtil.toHex(mutation.getRow())+" magically appeared",found);
        }
        for(KVPair pair:pairs){
            boolean found = false;
            for(Mutation mutation: successfulPuts){
                found = Bytes.equals(mutation.getRow(),pair.getRow());
                if(found)
                    break;
            }
            Assert.assertTrue("Row "+ BytesUtil.toHex(pair.getRow())+" magically appeared",found);
        }
    }

    @Test
    public void testNotServingRegionExceptionThrowingCausesAllRowsToFail() throws Exception {
        List<Mutation> results = Lists.newArrayList();

        HRegion testRegion = getMockRegion(getNotServingRegionAnswer());

        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));

        List<KVPair> pairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            pairs.add(next);
            testContext.sendUpstream(next);
        }

        Map<KVPair, WriteResult> finish = testContext.finish();

        //make sure no one got written
        Assert.assertEquals("Some rows got written, even though the region is closed!",0,results.size());
        for(WriteResult result:finish.values()){
            Assert.assertEquals("Row did not return correct code!", WriteResult.Code.NOT_SERVING_REGION,result.getCode());
        }
    }

    @Test
    public void testClosingRegionBeforeSendingUpstreamResultsInNotServingRegionCodes() throws Exception {
        List<Mutation> results = Lists.newArrayList();

        HRegion testRegion = getMockRegion(getSuccessOnlyAnswer(results));
        when(testRegion.isClosed()).thenReturn(true);

        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));

        List<KVPair> pairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            pairs.add(next);
            testContext.sendUpstream(next);
        }

        Map<KVPair, WriteResult> finish = testContext.finish();

        //make sure no one got written
        Assert.assertEquals("Some rows got written, even though the region is closed!",0,results.size());
        for(WriteResult result:finish.values()){
            Assert.assertEquals("Row did not return correct code!", WriteResult.Code.NOT_SERVING_REGION,result.getCode());
        }
    }

    @Test
    public void testWrongRegionIsProperlyReturned() throws Exception {
        List<Mutation> results = Lists.newArrayList();

        HRegion testRegion = getMockRegion(getSuccessOnlyAnswer(results));

        HRegionInfo info = testRegion.getRegionInfo();
        when(info.getEndKey()).thenReturn(Bytes.toBytes(11));

        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));

        List<KVPair> successfulPairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            successfulPairs.add(next);
            testContext.sendUpstream(next);
        }

        //close the region
        List<KVPair> failedPairs = Lists.newArrayList();
        for(int i=11;i<20;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            failedPairs.add(next);
            testContext.sendUpstream(next);
        }

        Map<KVPair, WriteResult> finish = testContext.finish();

        //make sure the correct number of rows got written
        Assert.assertEquals("Incorrect number of rows written!",successfulPairs.size(),results.size());

        //make sure every correct row shows up in results AND has the correct code
        for(KVPair success:successfulPairs){
            Assert.assertEquals("Incorrect return code!", WriteResult.Code.SUCCESS,finish.get(success).getCode());
            boolean found = false;
            for(Mutation mutation:results){
                found = Bytes.equals(mutation.getRow(),success.getRow());
                if(found)
                    break;
            }
            Assert.assertTrue("Row not present in results!", found);
        }

        //make sure every failed row has good code AND isn't in results
        for(KVPair failure:failedPairs){
            Assert.assertEquals("Incorrect return code!", WriteResult.Code.WRONG_REGION,finish.get(failure).getCode());
            boolean found = false;
            for(Mutation mutation:results){
                found = Bytes.equals(mutation.getRow(), failure.getRow());
                if(found)
                    break;
            }
            Assert.assertFalse("Row present in results!",found);
        }
    }

    @Test
    public void testClosingRegionHalfwayThroughUpstreamWritesHalfTheRecords() throws Exception {
        List<Mutation> results = Lists.newArrayList();

        HRegion testRegion = getMockRegion(getSuccessOnlyAnswer(results));

        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));

        List<KVPair> successfulPairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            successfulPairs.add(next);
            testContext.sendUpstream(next);
        }

        //close the region
        when(testRegion.isClosing()).thenReturn(true);
        List<KVPair> failedPairs = Lists.newArrayList();
        for(int i=11;i<20;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            failedPairs.add(next);
            testContext.sendUpstream(next);
        }

        Map<KVPair, WriteResult> finish = testContext.finish();

        //make sure the correct number of rows got written
        Assert.assertEquals("Incorrect number of rows written!",successfulPairs.size(),results.size());

        //make sure every correct row shows up in results AND has the correct code
        for(KVPair success:successfulPairs){
            Assert.assertEquals("Incorrect return code!", WriteResult.Code.SUCCESS,finish.get(success).getCode());
            boolean found = false;
            for(Mutation mutation:results){
                found = Bytes.equals(mutation.getRow(),success.getRow());
                if(found)
                    break;
            }
            Assert.assertTrue("Row not present in results!", found);
        }

        //make sure every failed row has good code AND isn't in results
        for(KVPair failure:failedPairs){
            Assert.assertEquals("Incorrect return code!", WriteResult.Code.NOT_SERVING_REGION,finish.get(failure).getCode());
            boolean found = false;
            for(Mutation mutation:results){
                found = Bytes.equals(mutation.getRow(), failure.getRow());
                if(found)
                    break;
            }
            Assert.assertFalse("Row present in results!",found);
        }
    }


}
