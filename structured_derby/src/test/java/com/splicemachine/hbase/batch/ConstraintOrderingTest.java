package com.splicemachine.hbase.batch;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintHandler;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.tools.ResettableCountDownLatch;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import static com.splicemachine.hbase.MockRegion.getMockRegion;
import static com.splicemachine.hbase.MockRegion.getSuccessOnlyAnswer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to ensure that rows are not written if constraints are violated, etc.
 *
 * @author Scott Fines
 * Created on: 9/26/13
 */
public class ConstraintOrderingTest {

    @Test
    public void testUniqueConstraintPreventsWrites() throws Exception {
        final ObjectArrayList<Mutation> successfulPuts = ObjectArrayList.newInstance();
        HRegion testRegion = getMockRegion(getSuccessOnlyAnswer(successfulPuts));
        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        when(rce.getRegion()).thenReturn(testRegion);

        final PipelineWriteContext testContext = new PipelineWriteContext("1",rce);
        Constraint uniqueConstraint = mock(Constraint.class);
        //noinspection unchecked
        when(uniqueConstraint.validate(any(KVPair.class),any(String.class),any(RegionCoprocessorEnvironment.class),anyCollection()))
                .thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocation) throws Throwable {
                        KVPair pair = (KVPair) invocation.getArguments()[0];
                        Object[] buffer = successfulPuts.buffer;
                        for (int i =0 ; i<successfulPuts.size(); i++) {
                        	Mutation mutation = (Mutation) buffer[i];
                            if (Bytes.equals(pair.getRow(), mutation.getRow())) {
                                return false;
                            }
                        }
                        @SuppressWarnings("unchecked") Collection<KVPair> otherBatch = (Collection<KVPair>) invocation.getArguments()[3];
                        for (KVPair kvPair : otherBatch) {
                            if (Bytes.equals(kvPair.getRow(), pair.getRow())) {
                                return false;
                            }
                        }

                        return true;
                    }
                });
        ConstraintContext constraintCtx = mock(ConstraintContext.class);
        when(constraintCtx.getTableName()).thenReturn("1184");
        when(constraintCtx.getConstraintName()).thenReturn("unique");
        when(uniqueConstraint.getConstraintContext()).thenReturn(constraintCtx);
        when(uniqueConstraint.getType()).thenReturn(Constraint.Type.UNIQUE);
        testContext.addLast(new RegionWriteHandler(testRegion,new ResettableCountDownLatch(0),100));
        testContext.addLast(new ConstraintHandler(uniqueConstraint));

        List<KVPair> pairs = Lists.newArrayList();
        for(int i=0;i<10;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i));
            pairs.add(next);
            testContext.sendUpstream(next);
        }
        System.out.println("Adding bad rows");
        List<KVPair> failed = Lists.newArrayList();
        for(int i=0;i<5;i++){
            KVPair next = new KVPair(Bytes.toBytes(i),Bytes.toBytes(i*2));
            failed.add(next);
            testContext.sendUpstream(next);
        }

        //make sure that nothing has been written to the region
        Assert.assertEquals("Writes have made it to the region!", 0, successfulPuts.size());

        //finish
        Map<KVPair,WriteResult> finish = testContext.finish();

        //make sure that the finish has the correct (successful) WriteResult
        Assert.assertEquals("Incorrect number of responses returned!",pairs.size(),finish.size());

        //make sure that all the KVs made it to the region
        Assert.assertEquals("incorrect number of rows made it to the region!",pairs.size()-failed.size(),successfulPuts.size());

        for(KVPair original:pairs){
            if(failed.contains(original)){
                Assert.assertEquals("Incorrect returned code!",WriteResult.Code.UNIQUE_VIOLATION,finish.get(original).getCode());

                //make sure that no row exists with that key
                //find it in the Mutations list
                int foundCount=0;
                Object[] buffer = successfulPuts.buffer;
                for (int i = 0 ; i<successfulPuts.size(); i++) {
                	Mutation mutation = (Mutation) buffer[i];
                    if(Bytes.equals(mutation.getRow(),original.getRow())){
                        foundCount++;
                        //make sure the rows match
                        KeyValue keyValue = ((Put) mutation).get(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY).get(0);
                        if(Bytes.equals(keyValue.getValue(),original.getValue()))
                            Assert.fail("Same Row key with incorrect value is present in final list!");
                    }
                }
                Assert.assertEquals("Incorrect number of rows written!",0,foundCount);
            }else{
                WriteResult result = finish.get(original);
                Assert.assertEquals("Incorrect result!",WriteResult.Code.SUCCESS,result.getCode());

                //find it in the Mutations list
                int foundCount=0;
                Object[] buffer = successfulPuts.buffer;
                for (int i = 0 ; i<successfulPuts.size(); i++) {                
                	Mutation mutation = (Mutation) buffer[i];
                	if(Bytes.equals(mutation.getRow(),original.getRow())){
                        foundCount++;
                        //make sure the rows match
                        KeyValue keyValue = ((Put) mutation).get(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY).get(0);
                        if(!Bytes.equals(keyValue.getValue(),original.getValue()))
                            Assert.fail("Same Row key with incorrect value is present in final list!");
                    }
                }
                Assert.assertEquals("Incorrect number of rows written!",1,foundCount);
            }
        }

    }
}
