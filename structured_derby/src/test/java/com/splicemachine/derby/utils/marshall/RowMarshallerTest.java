package com.splicemachine.derby.utils.marshall;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.*;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Created on: 10/2/13
 */
public class RowMarshallerTest {

    @Test
    public void testCanDecodeSparseReturnCorrectly() throws Exception {
        /*
         * Regression test for observed TPCC issue
         */
        String binary = "\\xCE\\xD8\\xF5\\xFA\\xA3\\xDF\\x88\\x00\\xC4\\xE2\\x00\\x83\\x00\\xC4\\x83\\x00\\xDFY\\x98\\x00IE\\x00CPVKDCTRTK\\x00|ttkqytfcejlimg\\x00\\xE4`\\x00a \\x00\\xC0$\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x81\\x00\\x80\\x00sprsckzju\\x00fsn|dlzmq{hhvfisfcr\\x00mysvwrw|jnozwihhr{\\x00YP\\x00:72:33333\\x00327:7867;75785;:\\x00\\xEDA\\xBB\\xA2\\x87\\xC8\\x00QG\\x00kvrmxikqjixsggeo{xhhgshhuptzlljsgnnzotyppxmvzvmfld{xqotdikqxrk{trglyolwdipxsqhrtspzpxey{y|xtkiwi|vvfj{kusfgi|ikurrittyqnv|xtyjqe{mdxhswzykydsvlgiqsgp{mjgrivtrkdpny{xhdxlvpr|kyzhodnjm{qdqrsqp|l|pqjfeiktf|uhelf{dvqd{j{wofqlvcllhdf|ypqh|kgjodcfcqduqhvqyudo{isxeneqdykowgqyeupud{vm|sylyf|{lg|{feuqcvnvxu|y|gffjhulnzsd||{dqxuj|p";

        byte[] bytes = Bytes.toBytesBinary(binary);

        /*
         * Easiest way to simulate what the PredicateFilter will return is to just apply it to the full row
         */
        Predicate p1 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0, Encoding.encode(1250),true);
        Predicate p2 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,1,Encoding.encode(3),true);
        Predicate p3 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,2,Encoding.encode(1155),true);

        Predicate and = new AndPredicate(ObjectArrayList.from(p1, p2, p3));

        BitSet fieldsToReturn = new BitSet(1);
        fieldsToReturn.set(0,3);

        EntryPredicateFilter epf = new EntryPredicateFilter(fieldsToReturn, ObjectArrayList.from(and),true);

        EntryAccumulator accumulator = epf.newAccumulator();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
        decoder.set(bytes);

        boolean match = epf.match(decoder, accumulator);
        Assert.assertTrue("Doesn't match!", match);

        byte[] accumulatedBytes = accumulator.finish();
        Assert.assertNotNull("Null bytes returned!",accumulatedBytes);
        KeyValue kv = new KeyValue(Bytes.toBytesBinary("\\xC4\\xE2\\x00\\x83\\x00\\xC4\\x83"),
                SpliceConstants.DEFAULT_FAMILY_BYTES,
                RowMarshaller.PACKED_COLUMN_KEY,
                accumulatedBytes);

        DataValueDescriptor[] dvds = new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLInteger()
        };
        RowMarshaller.sparsePacked().decode(kv,dvds,null,decoder);

        Assert.assertFalse("First field is null!",dvds[0].isNull());
        Assert.assertEquals("Incorrect first field!",1250,dvds[0].getInt());
        Assert.assertFalse("Second field is null!",dvds[1].isNull());
        Assert.assertEquals("Incorrect second field!",3,dvds[1].getInt());
        Assert.assertFalse("Third field is null!",dvds[2].isNull());
        Assert.assertEquals("Incorrect third field!",1155,dvds[2].getInt());
    }


    @Test
    public void testCanPackAndUnpackCorrectly() throws Exception {
        DataValueDescriptor[] dvd = new DataValueDescriptor[]{new SQLInteger(1250)};

        MultiFieldEncoder encoder = MultiFieldEncoder.create(KryoPool.defaultPool(), 1);
        byte[] bytes = RowMarshaller.sparsePacked().encodeRow(dvd, null, encoder);

        DataValueDescriptor[] decoded = new DataValueDescriptor[]{new SQLInteger()};
        KeyValue kv = new KeyValue(Bytes.toBytesBinary("\\xC4\\xE2\\x00\\x83\\x00\\xC4\\x83"),
                SpliceConstants.DEFAULT_FAMILY_BYTES,
                RowMarshaller.PACKED_COLUMN_KEY,
                bytes);

        MultiFieldDecoder decoder = MultiFieldDecoder.create(KryoPool.defaultPool());
        RowMarshaller.sparsePacked().decode(kv,decoded,null,decoder);

        Assert.assertFalse("Field is null!",decoded[0].isNull());
        Assert.assertArrayEquals("Incorrect decoding!",dvd,decoded);
    }
}
