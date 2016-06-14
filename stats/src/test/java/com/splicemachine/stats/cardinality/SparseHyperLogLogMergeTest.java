package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 8/19/15
 */
public class SparseHyperLogLogMergeTest{

    /*
     * These tests test that merging two SparseHyperLogLog instances
     * result in an equivalent data structure as if the same instance was
     * used to collect all the data (instead of two separate instances which were merged).
     *
     * Unfortunately, these tests are somewhat sensitive to this process, because
     * when you use the SparseHyperLogLog, the buffering construct changes the accuracy
     * of the estimation--the more you buffer, the more accurate results are. In some cases,
     * the merge() process will convert to dense even though our internal structure may not have
     * fully filled our max sparse size. In that case, we will have worse error than if we
     * were to fill the max sparse size completely and  then converted. With precision values where
     * this may occur, these tests would result in a slight difference in estimates, which fails
     * spuriously. Thus, even though it is awkward and smacks of testing around a problem rather
     * than fixing it, we have to choose our precision carefully.
     */

    @Test
    public void leftSparseRightSparse() throws Exception{
        Hash64 hashFunction=HashFunctions.murmur2_64(0);
        SparseHyperLogLog l1 = new SparseHyperLogLog(8,hashFunction);
        SparseHyperLogLog l2 = new SparseHyperLogLog(8,hashFunction);

        SparseHyperLogLog unmerged = new SparseHyperLogLog(8,hashFunction);
        for(int i=0;i<16;i++){
            l1.update(i);
            l2.update(2*i);
            unmerged.update(i);
            unmerged.update(2*i);
        }
        Assert.assertTrue("Left is not sparse!",l1.isSparse());
        Assert.assertTrue("Right is not sparse!",l2.isSparse());

        l1.merge(l2);

        long estimate = l1.getEstimate();
        long correct = unmerged.getEstimate();
        Assert.assertEquals("incorrect estimate after merging!",correct,estimate);
    }

    @Test
    public void leftSparseRightDense() throws Exception{
        Hash64 hashFunction=HashFunctions.murmur2_64(0);
        SparseHyperLogLog l1 = new SparseHyperLogLog(5,hashFunction);
        SparseHyperLogLog l2 = (SparseHyperLogLog)l1.getClone();

        SparseHyperLogLog unmerged = new SparseHyperLogLog(5,hashFunction);
        for(int i=0;i<16;i++){
            l1.update(i);
            unmerged.update(i);
        }

        for(int i=0;i<1000;i++){
            l2.update(2*i);
            unmerged.update(2*i);
        }
        Assert.assertTrue("Left is not sparse!",l1.isSparse());
        Assert.assertFalse("Right is sparse!",l2.isSparse());

        l1.merge(l2);

        long estimate = l1.getEstimate();
        long correct = unmerged.getEstimate();
        Assert.assertEquals("incorrect estimate after merging!",correct,estimate);
    }


    @Test
    public void leftDenseRightSparse() throws Exception{
        Hash64 hashFunction=HashFunctions.murmur2_64(0);
        SparseHyperLogLog l1 = new SparseHyperLogLog(8,hashFunction);
        SparseHyperLogLog l2 = new SparseHyperLogLog(8,hashFunction);

        SparseHyperLogLog unmerged = new SparseHyperLogLog(8,hashFunction);
        for(int i=0;i<1000;i++){
            l1.update(i);
            unmerged.update(i);
        }

        for(int i=0;i<16;i++){
            l2.update(2*i);
            unmerged.update(2*i);
        }
        Assert.assertFalse("Left is sparse!",l1.isSparse());
        Assert.assertTrue("Right is not sparse!",l2.isSparse());

        l1.merge(l2);

        long estimate = l1.getEstimate();
        long correct = unmerged.getEstimate();
        Assert.assertEquals("incorrect estimate after merging!",correct,estimate);
    }

    @Test
    public void leftDenseRightDense() throws Exception{
        Hash64 hashFunction=HashFunctions.murmur2_64(0);
        SparseHyperLogLog l1 = new SparseHyperLogLog(5,hashFunction);
        SparseHyperLogLog l2 = (SparseHyperLogLog)l1.getClone();

        SparseHyperLogLog unmerged = (SparseHyperLogLog)l1.getClone();
        for(int i=0;i<1000;i++){
            l1.update(i);
            unmerged.update(i);
        }

        for(int i=0;i<1000;i++){
            l2.update(2*i);
            unmerged.update(2*i);

        }


        Assert.assertFalse("Left is sparse!",l1.isSparse());
        Assert.assertFalse("Right is sparse!",l2.isSparse());

        l1.merge(l2);

        long estimate = l1.getEstimate();
        long correct = unmerged.getEstimate();
        Assert.assertEquals("incorrect estimate after merging!",correct,estimate);
    }
}